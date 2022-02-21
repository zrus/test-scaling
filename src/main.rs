use std::sync::{Arc, RwLock};

use anyhow::Error;
use bastion::prelude::*;
use byte_slice_cast::*;
use derive_more::{Display, Error};
use gst::{
    element_error,
    glib::{self, clone::Downgrade},
    prelude::{
        Cast, ElementExt, ElementExtManual, GObjectExtManualGst, GstBinExt, GstBinExtManual,
        GstObjectExt, ObjectExt, PadExt,
    },
    PadExtManual,
};
use gst_app::AppSink;
use gstreamer as gst;
use gstreamer_app as gst_app;

pub enum Event {
    FPS(u8),
}

impl Event {
    const EVENT_NAME: &'static str = "change-filter";

    #[allow(clippy::new_ret_no_self)]
    pub fn new_fps(fps: u8) -> gst::Event {
        let s = gst::Structure::builder(Self::EVENT_NAME)
            .field("fps", &fps)
            .build();
        gst::event::CustomUpstream::new(s)
    }

    pub fn parse(ev: &gst::EventRef) -> Option<Event> {
        match ev.view() {
            gst::EventView::CustomUpstream(e) => {
                let s = match e.structure() {
                    Some(s) if s.name() == Self::EVENT_NAME => s,
                    _ => return None,
                };
                let fps = s.get::<u8>("fps").unwrap();
                Some(Event::FPS(fps))
            }
            _ => None,
        }
    }
}

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
        // "rtsp://10.50.31.171/1/h264major",
        // "rtsp://10.50.13.231/1/h264major",
        // "rtsp://10.50.13.233/1/h264major",
        // "rtsp://10.50.13.234/1/h264major",
        // "rtsp://10.50.13.235/1/h264major",
        // "rtsp://10.50.13.236/1/h264major",
        // "rtsp://10.50.13.237/1/h264major",
        // "rtsp://10.50.13.238/1/h264major",
        // "rtsp://10.50.13.239/1/h264major",
        "rtsp://10.50.13.240/1/h264major",
        // "rtsp://10.50.13.241/1/h264major",
        // "rtsp://10.50.13.242/1/h264major",
        // "rtsp://10.50.13.243/1/h264major",
        // "rtsp://10.50.13.244/1/h264major",
        // "rtsp://10.50.13.245/1/h264major",
        // "rtsp://10.50.13.248/1/h264major",
        // "rtsp://10.50.13.249/1/h264major",
        // "rtsp://10.50.13.252/1/h264major",
        // "rtsp://10.50.13.253/1/h264major",
        // "rtsp://10.50.13.254/1/h264major",
    ];

    Bastion::init();
    Bastion::start();

    for url in cam_list {
        Bastion::children(move |children| {
            children
                .with_distributor(Distributor::named(url))
                .with_exec(move |ctx| async move {
                    // let pipeline = match create_pipeline(url) {
                    //     Ok(pl) => pl,
                    //     Err(_) => return Err(()),
                    // };
                    let mut is_fps_updated: Arc<RwLock<Option<i32>>> = Arc::new(RwLock::new(None));
                    loop {
                        // let pl_weak = ObjectExt::downgrade(&pipeline);
                        let is_fps_updated_weak = Downgrade::downgrade(&is_fps_updated);
                        MessageHandler::new(ctx.recv().await?)
                            .on_tell(|cmd: &str, _| {
                                // let pl_weak = pl_weak.clone();
                                let is_fps_updated_weak = is_fps_updated_weak.clone();
                                match cmd {
                                    "start" => {
                                        spawn! { async move {
                                            // let pipeline = match pl_weak.upgrade() {
                                            //     Some(pl) => pl,
                                            //     None => return
                                            // };
                                            let is_fps_updated = match is_fps_updated_weak.upgrade() {
                                                Some(uf) => uf,
                                                None => return
                                            };
                                            create_pipeline(url).and_then(|pipeline| main_loop(pipeline, is_fps_updated));
                                        }};
                                    }
                                    _ => {}
                                }
                            })
                            .on_tell(|fps: i32, _| {
                                println!("Change fps");
                                // let pl_weak = pl_weak.clone();
                                // let pipeline = match pl_weak.upgrade() {
                                //     Some(pl) => pl,
                                //     None => return,
                                // };
                                // set_framerate(pipeline, fps);
                                *is_fps_updated.write().unwrap() = Some(fps);
                            });
                    }
                })
        })
        .expect("");
        Distributor::named(url).tell_one("start");
        std::thread::sleep(std::time::Duration::from_secs(5));
        Distributor::named(url).tell_one(5);
    }

    Bastion::block_until_stopped();
}

fn create_pipeline(url: &str) -> Result<gst::Pipeline, Error> {
    println!("Pipeline {}", url);
    gst::init()?;

    let pipeline = gst::Pipeline::new(None)
        .downcast::<gst::Pipeline>()
        .expect("Expected a gst::Pipeline");
    let src = gst::ElementFactory::make("rtspsrc", None)?;
    src.set_property("location", url);

    let rtph264depay = gst::ElementFactory::make("rtph264depay", None)?;
    let queue =
        gst::ElementFactory::make("queue", Some("queue")).expect("Could not create queue element.");
    queue.set_property_from_str("leaky", "upstream");
    let queue_2 = gst::ElementFactory::make("queue", Some("queue_2"))
        .expect("Could not create queue element.");
    queue_2.set_property_from_str("leaky", "upstream");
    let queue_3 = gst::ElementFactory::make("queue", Some("queue_3"))?;
    let h264parse = gst::ElementFactory::make("h264parse", None)?;
    let vaapih264dec = gst::ElementFactory::make("vaapih264dec", None)?;
    let videorate = gst::ElementFactory::make("videorate", Some("videorate"))?;
    let vaapipostproc = gst::ElementFactory::make("vaapipostproc", None)?;
    let vaapijpegenc = gst::ElementFactory::make("vaapijpegenc", None)?;

    let capsfilter = gst::ElementFactory::make("capsfilter", Some("filter"))?;
    let caps = gst::Caps::builder("video/x-raw")
        .field("framerate", gst::Fraction::new(1, 1))
        .build();
    capsfilter.set_property("caps", &caps);
    let sink = gst::ElementFactory::make("appsink", Some("sink"))?;

    let elements = &[
        &src,
        &rtph264depay,
        &queue,
        &h264parse,
        &queue_2,
        &vaapih264dec,
        &videorate,
        &capsfilter,
        &queue_3,
        &vaapipostproc,
        &vaapijpegenc,
        &sink,
    ];
    pipeline.add_many(elements);

    println!("111111111111111111");
    // sink.link(&src)?;
    let rtph264depay_2 = rtph264depay.clone();

    src.connect_pad_added(move |_, src_pad| {
        // Obtain the sink_pad from audioconvert element
        let sink_pad = &rtph264depay
            .static_pad("sink")
            .expect("Failed to get static sink pad from convert");
        if sink_pad.is_linked() {
            println!("We are already linked. Ignoreing");
            return;
        }
        // Link the src pad to sink pad
        let res = src_pad.link(sink_pad);
        if res.is_err() {
            println!("Type is but link failed");
        } else {
            println!("Link succeeded type")
        }
    });
    rtph264depay_2.link(&queue).unwrap();
    queue.link(&h264parse).unwrap();
    h264parse.link(&queue_2).unwrap();
    queue_2.link(&vaapih264dec).unwrap();
    vaapih264dec.link(&videorate).unwrap();
    videorate.link(&capsfilter).unwrap();
    capsfilter.link(&queue_3).unwrap();
    queue_3.link(&vaapipostproc).unwrap();
    vaapipostproc.link(&vaapijpegenc).unwrap();
    vaapijpegenc.link(&sink).unwrap();
    println!("222222222222222222");

    // Tell the appsink what format we want. It will then be the audiotestsrc's job to
    // provide the format we request.
    // This can be set after linking the two objects, because format negotiation between
    // both elements will happen during pre-rolling of the pipeline.

    // println!("pipeline: {:?}", pipeline);
    // Get access to the appsink element.
    let appsink = pipeline
        .by_name("sink")
        .expect("Sink element not found")
        .downcast::<gst_app::AppSink>()
        .expect("Sink element is expected to be an appsink!");

    // appsink.set_caps(Some(
    //     &gst::Caps::builder("video/x-raw")
    //         .field("max-buffer", 100)
    //         .field("emit-signals", false)
    //         .field("drop", true)
    //         .build(),
    // ));

    appsink.set_property("emit-signals", false);
    appsink.set_property("max-buffers", 1u32);
    appsink.set_property("drop", true);

    let pipeline_weak = ObjectExt::downgrade(&pipeline);

    // Getting data out of the appsink is done by setting callbacks on it.
    // The appsink will then call those handlers, as soon as data is available.
    let url = url.to_owned();
    appsink.set_callbacks(
        gst_app::AppSinkCallbacks::builder()
            // Add a handler to the "new-sample" signal.
            .new_sample(move |appsink| callback(appsink, &url, "thumbnail"))
            .build(),
    );

    Ok(pipeline)
}

fn main_loop(
    pipeline: gst::Pipeline,
    is_fps_updated: Arc<RwLock<Option<i32>>>,
) -> Result<(), Error> {
    pipeline.set_state(gst::State::Playing)?;

    let bus = pipeline
        .bus()
        .expect("Pipeline without bus. Shouldn't happen!");

    use gst::MessageView;
    for msg in bus.iter_timed(gst::ClockTime::NONE) {
        match msg.view() {
            MessageView::Eos(msg) => {
                println!("Got Eos message: {:?}, done", msg);
                break;
            }
            MessageView::Error(err) => {
                pipeline.set_state(gst::State::Null)?;
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
            _ if is_fps_updated.read().unwrap().is_some() => {
                if let Some(fps) = *is_fps_updated.read().unwrap() {
                    pipeline.set_state(gst::State::Paused)?;

                    let capsfilter = pipeline
                        .by_name("filter")
                        .expect("Capsfilter not found")
                        .downcast::<gst::Element>()
                        .expect("Capsfilter is expected to be an appsink!");
                    let queue_3 = pipeline
                        .by_name("queue_3")
                        .expect("Queue 3 not found")
                        .downcast::<gst::Element>()
                        .expect("Queue 3 is expected to be an appsink!");
                    let videorate = pipeline
                        .by_name("videorate")
                        .expect("Videorate not found")
                        .downcast::<gst::Element>()
                        .expect("Videorate is expected to be an appsink!");

                    gstreamer::Element::unlink(&capsfilter, &queue_3);
                    gstreamer::Element::unlink(&videorate, &capsfilter);
                    capsfilter.set_state(gstreamer::State::Null)?;
                    pipeline.remove(&capsfilter)?;

                    let capsfilter_2 = gst::ElementFactory::make("capsfilter", Some("capsfilter"))?;
                    let caps = gst::Caps::builder("video/x-raw")
                        .field("framerate", gst::Fraction::new(fps, 1))
                        .build();
                    capsfilter_2.set_property("caps", &caps);

                    // videorate.link(&capsfilter_2).unwrap();
                    // capsfilter_2.link(&queue_3).unwrap();
                    pipeline.add(&capsfilter_2)?;
                    gstreamer::Element::link(&videorate, &capsfilter_2)?;
                    gstreamer::Element::link(&capsfilter_2, &queue_3)?;
                    *is_fps_updated.write().unwrap() = None;

                    pipeline.set_state(gst::State::Playing)?;
                }
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

    let _samples = map.as_slice_of::<u8>().map_err(|_| {
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

fn set_framerate(pipeline: gst::Pipeline, new_framerate: i32) -> gst::Pipeline {
    let filter = pipeline
        .by_name("filter")
        .expect("Cannot find any element named filter")
        .downcast::<gst::Element>()
        .expect("Cannot downcast filter to element");

    let new_caps = gst::Caps::new_simple(
        "video/x-raw",
        &[("framerate", &gst::Fraction::new(new_framerate, 1))],
    );

    filter.set_property("caps", &new_caps);

    pipeline
}
