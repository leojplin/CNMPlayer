use super::{PlaybackRuntimeState, PlaybackTrack};
use crate::data::config::CacheConfig;
use std::path::Path;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct MprisSyncPayload {
    pub playback: PlaybackRuntimeState,
    pub position: Duration,
    pub track: Option<PlaybackTrack>,
}

#[derive(Debug, Clone, Copy)]
pub enum MprisControlEvent {
    Play,
    Pause,
    PlayPause,
    Stop,
    Next,
    Previous,
    SeekRelativeMicros(i64),
    SeekAbsoluteMicros(i64),
}

#[cfg(target_os = "linux")]
mod imp {
    use super::{CacheConfig, MprisControlEvent, MprisSyncPayload, Path};
    use crate::app::player::cleanup_cache_dir;
    use crate::launch;
    use mpris_server::{Metadata, PlaybackStatus, Player, Time, zbus};
    use std::collections::hash_map::DefaultHasher;
    use std::fs;
    use std::hash::{Hash, Hasher};
    use std::sync::mpsc::{self as std_mpsc, Receiver, Sender};
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    pub struct MprisBridge {
        tx: see::unsync::Sender<Option<MprisSyncPayload>>,
        event_rx: Receiver<MprisControlEvent>,
    }

    impl MprisBridge {
        pub fn new(cache_root: &Path, cache_policy: &CacheConfig) -> Self {
            let art_dir = cache_root.join("mpris_art");
            let _ = fs::create_dir_all(&art_dir);
            let _ = cleanup_cache_dir(&art_dir, cache_policy);

            let (tx, mut rx) = see::unsync::channel(None);
            let (event_tx, event_rx) = std_mpsc::channel::<MprisControlEvent>();
            let cache_policy = cache_policy.clone();

            let task = async move {
                let player = match Player::builder("cnmplayer")
                    .can_play(true)
                    .can_pause(true)
                    .can_seek(true)
                    .can_go_next(true)
                    .can_go_previous(true)
                    .build()
                    .await
                {
                    Ok(player) => player,
                    Err(err) => {
                        log::warn!("mpris player init failed: {err}");
                        return;
                    }
                };

                bind_control_callbacks(&player, &event_tx);

                launch(player.run());

                while rx.changed().await.is_ok() {
                    let Some(payload) = rx.borrow_and_update().clone() else {
                        continue;
                    };

                    if let Err(err) =
                        apply_snapshot(&player, &art_dir, &cache_policy, payload).await
                    {
                        log::debug!("mpris sync failed: {err}");
                    }
                }
            };
            launch(task);

            Self { tx, event_rx }
        }

        pub fn update(&self, payload: MprisSyncPayload) {
            let _ = self.tx.send_replace(Some(payload));
        }

        pub fn pump(&self) {}

        pub fn drain_control_events(&self) -> Vec<MprisControlEvent> {
            let mut out = Vec::new();
            while let Ok(ev) = self.event_rx.try_recv() {
                out.push(ev);
            }
            out
        }
    }

    fn bind_control_callbacks(player: &Player, event_tx: &Sender<MprisControlEvent>) {
        let tx = event_tx.clone();
        player.connect_play(move |_| {
            let _ = tx.send(MprisControlEvent::Play);
        });

        let tx = event_tx.clone();
        player.connect_pause(move |_| {
            let _ = tx.send(MprisControlEvent::Pause);
        });

        let tx = event_tx.clone();
        player.connect_play_pause(move |_| {
            let _ = tx.send(MprisControlEvent::PlayPause);
        });

        let tx = event_tx.clone();
        player.connect_stop(move |_| {
            let _ = tx.send(MprisControlEvent::Stop);
        });

        let tx = event_tx.clone();
        player.connect_next(move |_| {
            let _ = tx.send(MprisControlEvent::Next);
        });

        let tx = event_tx.clone();
        player.connect_previous(move |_| {
            let _ = tx.send(MprisControlEvent::Previous);
        });

        let tx = event_tx.clone();
        player.connect_seek(move |_, offset| {
            let _ = tx.send(MprisControlEvent::SeekRelativeMicros(offset.as_micros()));
        });

        let tx = event_tx.clone();
        player.connect_set_position(move |_, _, position| {
            let _ = tx.send(MprisControlEvent::SeekAbsoluteMicros(position.as_micros()));
        });
    }

    async fn apply_snapshot(
        player: &Player,
        art_dir: &Path,
        cache_policy: &CacheConfig,
        payload: MprisSyncPayload,
    ) -> zbus::Result<()> {
        player
            .set_playback_status(match payload.playback {
                super::PlaybackRuntimeState::Playing => PlaybackStatus::Playing,
                super::PlaybackRuntimeState::Paused => PlaybackStatus::Paused,
                super::PlaybackRuntimeState::Stopped => PlaybackStatus::Stopped,
            })
            .await?;

        player.set_position(time_from_duration(payload.position));

        if let Some(track) = payload.track {
            player
                .set_metadata(build_metadata(art_dir, cache_policy, &track))
                .await?;
        }

        Ok(())
    }

    fn build_metadata(
        art_dir: &Path,
        cache_policy: &CacheConfig,
        track: &super::PlaybackTrack,
    ) -> Metadata {
        let mut metadata = Metadata::new();

        if !track.title.trim().is_empty() {
            metadata.set_title(Some(track.title.clone()));
        }
        if !track.artist.trim().is_empty() {
            metadata.set_artist(Some([track.artist.clone()]));
            metadata.set_album_artist(Some([track.artist.clone()]));
        }
        if !track.album.trim().is_empty() {
            metadata.set_album(Some(track.album.clone()));
        }
        if track.duration_ms > 0 {
            metadata.set_length(Some(Time::from_micros(
                track
                    .duration_ms
                    .saturating_mul(1000)
                    .clamp(i64::MIN / 2, i64::MAX / 2),
            )));
        }

        if !track.song_id.trim().is_empty() {
            metadata.set_url(Some(format!(
                "https://music.163.com/#/song?id={}",
                track.song_id
            )));
            metadata.set_comment(Some([format!("song_id={}", track.song_id)]));
        }

        if let Some(lyrics) = &track.lyrics {
            if !lyrics.is_empty() {
                let text = lyrics
                    .iter()
                    .map(|line| line.text.trim())
                    .filter(|line| !line.is_empty())
                    .collect::<Vec<_>>()
                    .join("\n");
                if !text.is_empty() {
                    metadata.set_lyrics(Some(text));
                }
            }
        }

        if let Some(bytes) = track.cover.as_deref() {
            if !bytes.is_empty() {
                if let Some(art_url) =
                    persist_cover_as_file_url(art_dir, cache_policy, &track.song_id, bytes)
                {
                    metadata.set_art_url(Some(art_url));
                }
            }
        }

        metadata
    }

    fn persist_cover_as_file_url(
        art_dir: &Path,
        cache_policy: &CacheConfig,
        song_id: &str,
        bytes: &[u8],
    ) -> Option<String> {
        let mut hasher = DefaultHasher::new();
        bytes.hash(&mut hasher);
        let hash = hasher.finish();

        let safe_id = sanitize_for_filename(song_id);
        let filename = format!("{}_{}.img", safe_id, hash);
        let path = art_dir.join(filename);

        if !path.is_file() {
            fs::write(&path, bytes).ok()?;
            cleanup_art_dir_throttled(art_dir, cache_policy);
            // 清理可能按容量把刚写的文件也删了，补写一次。
            if !path.is_file() {
                fs::write(&path, bytes).ok()?;
            }
        }

        Some(format!("file://{}", path.to_string_lossy()))
    }

    /// `cleanup_cache_dir` 会 `read_dir` 整个目录并对每个文件取 metadata，
    /// 开销随封面数量线性增长。原先每写一张封面就调一次，等于每次切歌
    /// 全目录扫描。这里限制最短间隔，把它摊薄成周期性维护。
    fn cleanup_art_dir_throttled(art_dir: &Path, cache_policy: &CacheConfig) {
        const MIN_INTERVAL: Duration = Duration::from_secs(300);
        static LAST_RUN: Mutex<Option<Instant>> = Mutex::new(None);

        let Ok(mut last) = LAST_RUN.lock() else {
            return;
        };
        if let Some(at) = *last {
            if at.elapsed() < MIN_INTERVAL {
                return;
            }
        }
        *last = Some(Instant::now());
        drop(last);

        let _ = cleanup_cache_dir(art_dir, cache_policy);
    }

    fn sanitize_for_filename(input: &str) -> String {
        let mut out = String::with_capacity(input.len());
        for ch in input.chars() {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                out.push(ch);
            }
        }
        if out.is_empty() {
            "track".to_string()
        } else {
            out
        }
    }

    fn time_from_duration(dur: std::time::Duration) -> Time {
        let micros_u128 = dur.as_micros();
        let micros = if micros_u128 > i64::MAX as u128 {
            i64::MAX
        } else {
            micros_u128 as i64
        };
        Time::from_micros(micros)
    }
}

#[cfg(target_os = "macos")]
mod imp {
    use super::{
        CacheConfig, MprisControlEvent, MprisSyncPayload, Path, PlaybackRuntimeState,
    };
    use block2::RcBlock;
    use objc2::msg_send;
    use objc2::rc::Retained;
    use objc2::runtime::{AnyClass, AnyObject};
    use objc2_foundation::{NSDate, NSMutableDictionary, NSNumber, NSRunLoop, NSString};
    use objc2_media_player::{
        MPMediaItemPropertyAlbumTitle, MPMediaItemPropertyArtist,
        MPMediaItemPropertyPlaybackDuration, MPMediaItemPropertyTitle,
        MPNowPlayingInfoCenter, MPNowPlayingInfoMediaType, MPNowPlayingInfoPropertyElapsedPlaybackTime,
        MPNowPlayingInfoPropertyMediaType, MPNowPlayingInfoPropertyPlaybackRate,
        MPNowPlayingPlaybackState, MPRemoteCommand,
        MPRemoteCommandCenter, MPRemoteCommandEvent, MPRemoteCommandHandlerStatus,
    };
    use std::ffi::c_void;
    use std::ptr::NonNull;
    use std::sync::OnceLock;
    use std::sync::mpsc::{self as std_mpsc, Receiver, Sender};

    type SetCanBeNowPlayingApplication = unsafe extern "C" fn(u8) -> u8;
    static SET_CAN_BE_NOW_PLAYING: OnceLock<Option<SetCanBeNowPlayingApplication>> = OnceLock::new();

    pub struct MprisBridge {
        // AppKit and MediaPlayer must be initialized and serviced on the process's
        // main thread. The main TUI loop calls `pump` between its own events.
        _application: Retained<AnyObject>,
        info_center: Retained<MPNowPlayingInfoCenter>,
        run_loop: Retained<NSRunLoop>,
        _handler_targets: Vec<Retained<AnyObject>>,
        event_rx: Receiver<MprisControlEvent>,
    }

    impl MprisBridge {
        pub fn new(_cache_root: &Path, _cache_policy: &CacheConfig) -> Self {
            let application = unsafe {
                let class = AnyClass::get(c"NSApplication")
                    .expect("NSApplication is unavailable on macOS");
                let app: Retained<AnyObject> = msg_send![class, sharedApplication];
                let _: () = msg_send![&app, finishLaunching];
                // NSApplicationActivationPolicyAccessory: stay out of the Dock while
                // remaining an eligible background media application.
                let _: bool = msg_send![&app, setActivationPolicy: 1isize];
                app
            };

            let (event_tx, event_rx) = std_mpsc::channel();
            let command_center = unsafe { MPRemoteCommandCenter::sharedCommandCenter() };
            let play_command = unsafe { command_center.playCommand() };
            let pause_command = unsafe { command_center.pauseCommand() };
            let toggle_command = unsafe { command_center.togglePlayPauseCommand() };
            let stop_command = unsafe { command_center.stopCommand() };
            let next_command = unsafe { command_center.nextTrackCommand() };
            let previous_command = unsafe { command_center.previousTrackCommand() };
            let handler_targets = vec![
                register_command(&play_command, &event_tx, MprisControlEvent::Play),
                register_command(&pause_command, &event_tx, MprisControlEvent::Pause),
                register_command(
                    &toggle_command,
                    &event_tx,
                    MprisControlEvent::PlayPause,
                ),
                register_command(&stop_command, &event_tx, MprisControlEvent::Stop),
                register_command(&next_command, &event_tx, MprisControlEvent::Next),
                register_command(
                    &previous_command,
                    &event_tx,
                    MprisControlEvent::Previous,
                ),
            ];

            log::info!("macOS media control handlers registered on main thread");
            Self {
                _application: application,
                info_center: unsafe { MPNowPlayingInfoCenter::defaultCenter() },
                run_loop: NSRunLoop::currentRunLoop(),
                _handler_targets: handler_targets,
                event_rx,
            }
        }

        pub fn update(&self, payload: MprisSyncPayload) {
            apply_snapshot(&self.info_center, payload);
        }

        pub fn pump(&self) {
            let deadline = NSDate::dateWithTimeIntervalSinceNow(0.01);
            self.run_loop.runUntilDate(&deadline);
        }

        pub fn drain_control_events(&self) -> Vec<MprisControlEvent> {
            self.event_rx.try_iter().collect()
        }
    }

    fn register_command(
        command: &MPRemoteCommand,
        event_tx: &Sender<MprisControlEvent>,
        event: MprisControlEvent,
    ) -> Retained<AnyObject> {
        let tx = event_tx.clone();
        let handler: RcBlock<
            dyn Fn(NonNull<MPRemoteCommandEvent>) -> MPRemoteCommandHandlerStatus,
        > = RcBlock::new(move |_| {
            log::debug!("macOS media control event: {event:?}");
            let _ = tx.send(event);
            MPRemoteCommandHandlerStatus::Success
        });

        unsafe {
            command.setEnabled(true);
            command.addTargetWithHandler(&handler)
        }
    }

    fn set_now_playing_eligibility(enabled: bool) {
        let setter = SET_CAN_BE_NOW_PLAYING.get_or_init(|| unsafe {
            let framework = libc::dlopen(
                c"/System/Library/PrivateFrameworks/MediaRemote.framework/MediaRemote".as_ptr(),
                libc::RTLD_LAZY,
            );
            if framework.is_null() {
                return None;
            }

            let symbol = libc::dlsym(
                framework,
                c"MRMediaRemoteSetCanBeNowPlayingApplication".as_ptr(),
            );
            if symbol.is_null() {
                return None;
            }

            Some(std::mem::transmute::<
                *mut c_void,
                SetCanBeNowPlayingApplication,
            >(symbol))
        });

        if let Some(setter) = setter {
            let accepted = unsafe { setter(enabled as u8) };
            if accepted == 0 {
                log::debug!("macOS MediaRemote rejected Now Playing eligibility");
            }
        }
    }

    fn apply_snapshot(info_center: &MPNowPlayingInfoCenter, payload: MprisSyncPayload) {
        if payload.playback == PlaybackRuntimeState::Stopped {
            set_now_playing_eligibility(false);
        } else if payload.track.is_some() {
            set_now_playing_eligibility(true);
        }

        let state = match payload.playback {
            PlaybackRuntimeState::Playing => MPNowPlayingPlaybackState::Playing,
            PlaybackRuntimeState::Paused => MPNowPlayingPlaybackState::Paused,
            PlaybackRuntimeState::Stopped => MPNowPlayingPlaybackState::Stopped,
        };

        unsafe {
            if let Some(track) = payload.track {
                let dict: Retained<NSMutableDictionary<NSString, AnyObject>> =
                    NSMutableDictionary::new();
                let title = NSString::from_str(&track.title);
                let artist = NSString::from_str(&track.artist);
                let album = NSString::from_str(&track.album);
                let duration =
                    NSNumber::numberWithDouble(track.duration_ms.max(0) as f64 / 1000.0);
                let media_type =
                    NSNumber::numberWithUnsignedInteger(MPNowPlayingInfoMediaType::Audio.0);
                let elapsed = NSNumber::numberWithDouble(payload.position.as_secs_f64());
                let rate = NSNumber::numberWithDouble(if payload.playback
                    == PlaybackRuntimeState::Playing
                {
                    1.0
                } else {
                    0.0
                });

                dict.insert(MPMediaItemPropertyTitle, &*title);
                dict.insert(MPMediaItemPropertyArtist, &*artist);
                dict.insert(MPMediaItemPropertyAlbumTitle, &*album);
                dict.insert(MPMediaItemPropertyPlaybackDuration, &*duration);
                dict.insert(MPNowPlayingInfoPropertyMediaType, &*media_type);
                dict.insert(MPNowPlayingInfoPropertyElapsedPlaybackTime, &*elapsed);
                dict.insert(MPNowPlayingInfoPropertyPlaybackRate, &*rate);
                info_center.setNowPlayingInfo(Some(&dict));
            } else if payload.playback == PlaybackRuntimeState::Stopped {
                info_center.setNowPlayingInfo(None);
            } else if let Some(existing) = info_center.nowPlayingInfo() {
                let dict: Retained<NSMutableDictionary<NSString, AnyObject>> =
                    NSMutableDictionary::dictionaryWithDictionary(&existing);
                let elapsed = NSNumber::numberWithDouble(payload.position.as_secs_f64());
                let rate = NSNumber::numberWithDouble(if payload.playback
                    == PlaybackRuntimeState::Playing
                {
                    1.0
                } else {
                    0.0
                });
                dict.insert(MPNowPlayingInfoPropertyElapsedPlaybackTime, &*elapsed);
                dict.insert(MPNowPlayingInfoPropertyPlaybackRate, &*rate);
                info_center.setNowPlayingInfo(Some(&dict));
            }
            info_center.setPlaybackState(state);
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod imp {
    use super::{CacheConfig, MprisControlEvent, MprisSyncPayload, Path};

    pub struct MprisBridge;

    impl MprisBridge {
        pub fn new(_cache_root: &Path, _cache_policy: &CacheConfig) -> Self {
            Self
        }

        pub fn update(&self, _payload: MprisSyncPayload) {}

        pub fn pump(&self) {}

        pub fn drain_control_events(&self) -> Vec<MprisControlEvent> {
            Vec::new()
        }
    }
}

pub use imp::MprisBridge;
