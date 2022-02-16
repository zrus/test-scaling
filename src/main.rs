use std::{io::BufWriter, num::NonZeroU32};

use anyhow::Error;
use bastion::prelude::*;
use byte_slice_cast::*;
use derive_more::{Display, Error};
use fast_image_resize as fr;
use gst::{
    element_error, glib,
    prelude::{Cast, ElementExt, GstBinExt, GstObjectExt},
};
use gst_app::AppSink;
use gstreamer as gst;
use gstreamer_app as gst_app;
use image::{self, ColorType, ImageFormat};

#[derive(Debug, Display, Error)]
#[display(fmt = "Received error from {}: {} (debug: {:?})", src, error, debug)]
struct ErrorMessage {
    src: String,
    error: String,
    debug: Option<String>,
    source: glib::Error,
}

fn main() {
    let cam_list = vec![
        "rtsp://10.50.31.171/1/h264major",
        "rtsp://10.50.13.231/1/h264major",
        "rtsp://10.50.13.233/1/h264major",
        "rtsp://10.50.13.234/1/h264major",
        "rtsp://10.50.13.235/1/h264major",
        "rtsp://10.50.13.236/1/h264major",
        "rtsp://10.50.13.237/1/h264major",
        "rtsp://10.50.13.238/1/h264major",
        "rtsp://10.50.13.239/1/h264major",
        "rtsp://10.50.13.240/1/h264major",
        "rtsp://10.50.13.241/1/h264major",
        "rtsp://10.50.13.242/1/h264major",
        "rtsp://10.50.13.243/1/h264major",
        "rtsp://10.50.13.244/1/h264major",
        "rtsp://10.50.13.245/1/h264major",
        "rtsp://10.50.13.248/1/h264major",
        "rtsp://10.50.13.249/1/h264major",
        "rtsp://10.50.13.252/1/h264major",
        "rtsp://10.50.13.253/1/h264major",
        "rtsp://10.50.13.254/1/h264major",
    ];

    Bastion::init();
    Bastion::start();

    for url in cam_list {
        Bastion::children(|children| {
            children.with_exec(move |ctx| async {
                blocking! { create_pipeline(url).and_then(|pipeline| main_loop(pipeline, url)); };
                loop {}
            })
        });
    }

    Bastion::block_until_stopped();
}

fn create_pipeline(url: &str) -> Result<gst::Pipeline, Error> {
    println!("Pipeline {}", url);
    gst::init()?;

    let pipeline = gst::parse_launch(&format!(
        "rtspsrc location={} !
        application/x-rtp, media=video, encoding-name=H264!
        rtph264depay ! queue leaky=2 !
        h264parse ! tee name=thumbnail_video !
        queue leaky=2 ! vaapih264dec !
        videorate ! video/x-raw, framerate=5/1 !
        vaapipostproc ! vaapijpegenc !
        appsink name=app1 max-buffers=100 emit-signals=false drop=true
        thumbnail_video. ! queue leaky=2 ! vaapih264dec !
        videorate ! video/x-raw, framerate=5/1 !
        vaapipostproc ! video/x-raw, width=720, height=480 ! vaapijpegenc !
        appsink name=app2 max-buffers=100 emit-signals=false drop=true",
        url
    ))?
    .downcast::<gst::Pipeline>()
    .expect("Expected Gst Pipeline");

    let appsink1 = pipeline
        .by_name("app1")
        .expect("Sink element not found")
        .downcast::<gst_app::AppSink>()
        .expect("Sink element was expected to be an appsink");

    let appsink2 = pipeline
        .by_name("app1")
        .expect("Sink element not found")
        .downcast::<gst_app::AppSink>()
        .expect("Sink element was expected to be an appsink");

    let url1 = url.to_owned();
    appsink1.set_callbacks(
        gst_app::AppSinkCallbacks::builder()
            .new_sample(move |appsink| callback(appsink, &url1, "fullscreen"))
            .build(),
    );

    let url2 = url.to_owned();
    appsink2.set_callbacks(
        gst_app::AppSinkCallbacks::builder()
            .new_sample(move |appsink| callback(appsink, &url2, "thumbnail"))
            .build(),
    );

    Ok(pipeline)
}

fn main_loop(pipeline: gst::Pipeline, url: &str) -> Result<(), Error> {
    println!("Start main loop {}", url);
    pipeline.set_state(gst::State::Playing)?;

    let bus = pipeline
        .bus()
        .expect("Pipeline without bus. Shouldn't happen!");

    use gst::MessageView;
    for msg in bus.iter_timed(gst::ClockTime::NONE) {
        match msg.view() {
            MessageView::Eos(..) => {
                println!("Got Eos message, done");
                break;
            }
            MessageView::Error(err) => {
                pipeline.set_state(gst::State::Null)?;
                println!("Error at {}: {:?}", url, err.error());
                return Err(ErrorMessage {
                    src: msg
                        .src()
                        .map(|s| String::from(s.path_string()))
                        .unwrap_or_else(|| String::from("None")),
                    error: err.error().to_string(),
                    debug: err.debug(),
                    source: err.error(),
                }
                .into());
            }
            _ => (),
        }
    }

    println!("Main loop break");

    pipeline.set_state(gst::State::Null)?;

    Ok(())
}

fn callback(
    appsink: &AppSink,
    url: &str,
    screen_type: &str,
) -> Result<gst::FlowSuccess, gst::FlowError> {
    let sample = appsink.pull_sample().map_err(|_| gst::FlowError::Eos)?;

    let buffer = sample.buffer().ok_or_else(|| {
        element_error!(
            appsink,
            gst::ResourceError::Failed,
            ("Failed to get buffer from appsink")
        );
        gst::FlowError::Error
    })?;

    let map = buffer.map_readable().map_err(|_| {
        element_error!(
            appsink,
            gst::ResourceError::Failed,
            ("Failed to map buffer readable")
        );
        gst::FlowError::Error
    })?;

    let samples = map.as_slice_of::<u8>().map_err(|_| {
        element_error!(
            appsink,
            gst::ResourceError::Failed,
            ("Failed to interprete buffer as S16 PCM")
        );
        gst::FlowError::Error
    })?;

    println!("{} - {}: {:?}", url, screen_type, std::time::Instant::now());

    Ok(gst::FlowSuccess::Ok)
}
