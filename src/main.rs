use std::{io::BufWriter, num::NonZeroU32};

use anyhow::Error;
use bastion::prelude::*;
use byte_slice_cast::*;
use derive_more::{Display, Error};
use fast_image_resize as fr;
use gst::{
    element_error, glib,
    prelude::{
        Cast, ElementExt, GObjectExtManualGst, GstBinExt, GstBinExtManual, GstObjectExt, ObjectExt,
        PadExt,
    },
};
use gst_app::AppSink;
use gstreamer as gst;
use gstreamer_app as gst_app;
use gstreamer_video as gst_video;
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
            children.with_exec(|ctx| async {
                spawn! { async { create_pipeline(url).and_then(|pipeline| main_loop(pipeline, url)); } };
                loop {}
            })
        });
    }

    Bastion::block_until_stopped();
}

fn create_pipeline(url: &str) -> Result<gst::Pipeline, Error> {
    println!("Pipeline {}", url);
    gst::init()?;

    // Initialize new raw pipeline
    let pipeline = gst::Pipeline::new(None);
    // Initialize RTSP source
    let src = gst::ElementFactory::make("rtspsrc", Some("src"))?;
    src.set_property("location", url);
    // Initialize rtph264depay
    let rtph264depay = gst::ElementFactory::make("rtph264depay", Some("depay"))?;
    src.link(&rtph264depay);
    println!("1");
    let rtph264depay_weak = rtph264depay.downgrade();
    src.connect_pad_added(move |_, src_pad| {
        let rtph264depay = match rtph264depay_weak.upgrade() {
            Some(depay) => depay,
            None => return,
        };
        let sink_pad = rtph264depay
            .static_pad("sink")
            .expect("rtph264depay has no sink pad");
        if sink_pad.is_linked() {
            return;
        }
        src_pad.link(&sink_pad);
    });
    println!("2");
    // Initialize queue 1
    let queue1 = gst::ElementFactory::make("queue", Some("queue1")).unwrap();
    queue1.set_property_from_str("leaky", "downstream");
    rtph264depay.link(&queue1)?;
    println!("3");
    // Initialize h264parse
    let h264parse = gst::ElementFactory::make("h264parse", None)?;
    queue1.link(&h264parse)?;
    println!("4");
    // Initialize queue 2
    let queue2 = gst::ElementFactory::make("queue", Some("queue2")).unwrap();
    queue2.set_property_from_str("leaky", "downstream");
    h264parse.link(&queue2)?;
    println!("5");
    // Initialize vaapih264dec
    let vaapih264dec = gst::ElementFactory::make("vaapih264dec", None)?;
    queue2.link(&vaapih264dec)?;
    println!("6");
    // Initialize videorate
    let videorate = gst::ElementFactory::make("videorate", None)?;
    vaapih264dec.link(&videorate)?;
    println!("7");
    // Initialize capsfilter for videorate
    let capsfilter = gst::ElementFactory::make("capsfilter", None)?;
    let caps = gst::Caps::new_simple(
        "video/x-raw",
        &[
            ("width", &720),
            ("height", &480),
            ("framerate", &gst::Fraction::new(5, 1)),
        ],
    );
    // capsfilter.set_property_from_str("caps", &format!("video/x-raw,framerate={}/1", 5));
    capsfilter.set_property("caps", &caps);
    videorate.link(&capsfilter)?;
    // videorate.link_filtered(
    //     &videorate,
    //     &gst::Caps::from(&format!("video/x-raw,framerate={}/1", 5)),
    // );
    println!("8");
    // Initialize vaapipostproc
    let vaapipostproc = gst::ElementFactory::make("vaapipostproc", None)?;
    capsfilter.link(&vaapipostproc)?;
    println!("9");
    // Initialize vaapijpegenc
    let vaapijpegenc = gst::ElementFactory::make("vaapijpegenc", None)?;
    vaapipostproc.link(&vaapijpegenc)?;
    println!("10");
    // Initialize appsink 1
    let sink1 = gst::ElementFactory::make("appsink", None)?;
    sink1.set_property_from_str("name", "app1");
    // sink1.set_property_from_str("max-buffers", "100");
    // sink1.set_property_from_str("emit-signals", "false");
    // sink1.set_property_from_str("drop", "true");
    vaapijpegenc.link(&sink1)?;
    println!("11");

    // // THUMNAIL
    // // Initialize tee
    // let tee = gst::ElementFactory::make("tee", None)?;
    // tee.set_property("name", "thumbnail");
    // h264parse.link(&tee)?;
    // println!("12");
    // // Initialize queue 2
    // let queue3 = gst::ElementFactory::make("queue", None).unwrap();
    // queue3.set_property_from_str("leaky", "downstream");
    // tee.link(&queue3)?;
    // println!("13");
    // // Initialize vaapih264dec
    // let vaapih264dec1 = gst::ElementFactory::make("vaapih264dec", None)?;
    // queue3.link(&vaapih264dec1)?;
    // println!("14");
    // // Initialize videorate
    // let videorate1 = gst::ElementFactory::make("videorate", None)?;
    // vaapih264dec1.link(&videorate1)?;
    // println!("15");
    // // Initialize capsfilter for videorate
    // let capsfilter1 = gst::ElementFactory::make("capsfilter", None)?;
    // capsfilter1.set_property_from_str("caps", &format!("video/x-raw,framerate={}/1", 5));
    // videorate1.link(&capsfilter1)?;
    // println!("16");
    // // Initialize vaapipostproc
    // let vaapipostproc1 = gst::ElementFactory::make("vaapipostproc", None)?;
    // capsfilter1.link(&vaapipostproc1)?;
    // vaapipostproc1.set_property_from_str("width", "720");
    // vaapipostproc1.set_property_from_str("height", "480");
    // println!("17");
    // // Initialize vaapijpegenc
    // let vaapijpegenc1 = gst::ElementFactory::make("vaapijpegenc", None)?;
    // vaapipostproc1.link(&vaapijpegenc1)?;
    // println!("18");
    // // Initialize AppSink 2
    // let sink2 = gst::ElementFactory::make("appsink", None)?;
    // sink2.set_property_from_str("name", "app2");
    // sink2.set_property_from_str("max-buffers", "100");
    // sink2.set_property_from_str("emit-signals", "false");
    // sink2.set_property_from_str("drop", "true");
    // vaapijpegenc1.link(&sink2)?;
    // println!("19");

    // ADD MANY ELEMENTS TO PIPELINE AND LINK THEM TOGETHER
    let elements = &[
        &src,
        &rtph264depay,
        &queue1,
        &h264parse,
        &queue2,
        &vaapih264dec,
        &videorate,
        &capsfilter,
        &vaapipostproc,
        &vaapijpegenc,
        &sink1,
        // &tee,
        // &queue3,
        // &vaapih264dec1,
        // &videorate1,
        // &capsfilter1,
        // &vaapipostproc1,
        // &vaapijpegenc1,
        // &sink2,
    ];
    pipeline.add_many(elements);
    println!("20");
    // gst::Element::link_many(elements);

    // let pipeline = gst::parse_launch(&format!(
    //     "rtspsrc location={} !
    //     application/x-rtp, media=video, encoding-name=H264!
    //     rtph264depay ! queue leaky=2 !
    //     h264parse ! tee name=thumbnail_video !
    //     queue leaky=2 ! vaapih264dec !
    //     videorate ! video/x-raw, framerate=5/1 !
    //     vaapipostproc ! vaapijpegenc !
    //     appsink name=app1 max-buffers=100 emit-signals=false drop=true
    //     thumbnail_video. ! queue leaky=2 ! vaapih264dec !
    //     videorate ! video/x-raw, framerate=5/1 !
    //     vaapipostproc ! video/x-raw, width=720, height=480 ! vaapijpegenc !
    //     appsink name=app2 max-buffers=100 emit-signals=false drop=true",
    //     url
    // ))?
    // .downcast::<gst::Pipeline>()
    // .expect("Expected Gst Pipeline");

    let appsink1 = pipeline
        .by_name("app1")
        .expect("Sink element not found")
        .downcast::<gst_app::AppSink>()
        .expect("Sink element was expected to be an appsink");
    println!("21");

    // let appsink2 = pipeline
    //     .by_name("app2")
    //     .expect("Sink element not found")
    //     .downcast::<gst_app::AppSink>()
    //     .expect("Sink element was expected to be an appsink");
    // println!("22");

    let url1 = url.to_owned();
    appsink1.set_callbacks(
        gst_app::AppSinkCallbacks::builder()
            .new_sample(move |appsink| callback(appsink, &url1, "fullscreen"))
            .build(),
    );
    println!("23");

    // let url2 = url.to_owned();
    // appsink2.set_callbacks(
    //     gst_app::AppSinkCallbacks::builder()
    //         .new_sample(move |appsink| callback(appsink, &url2, "thumbnail"))
    //         .build(),
    // );
    // println!("24");

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
