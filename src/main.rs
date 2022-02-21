use anyhow::Error;
use bastion::prelude::*;
use byte_slice_cast::*;
use derive_more::{Display, Error};
use gst::{
    element_error, glib,
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
        // "rtsp://10.50.13.240/1/h264major",
        // "rtsp://10.50.13.241/1/h264major",
        "rtsp://10.50.13.242/1/h264major",
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
                    let pipeline = match create_pipeline(url) {
                        Ok(pl) => pl,
                        Err(_) => return Err(()),
                    };
                    loop {
                        let pl_weak = pipeline.downgrade();
                        MessageHandler::new(ctx.recv().await?)
                            .on_tell(|cmd: &str, _| {
                                let pl_weak = pl_weak.clone();
                                match cmd {
                                    "start" => {
                                        spawn! { async move {
                                            let pipeline = match pl_weak.upgrade() {
                                                Some(pl) => pl,
                                                None => return
                                            };
                                            main_loop(pipeline);
                                        }};
                                    }
                                    _ => {}
                                }
                            })
                            .on_tell(|fps: i32, _| {
                                println!("Change fps");
                                let pl_weak = pl_weak.clone();
                                let pipeline = match pl_weak.upgrade() {
                                    Some(pl) => pl,
                                    None => return,
                                };

                                set_framerate(pipeline, fps);
                            });
                    }
                })
        })
        .expect("");
        Distributor::named(url).tell_one("start");
        std::thread::sleep(std::time::Duration::from_secs(5));
        Distributor::named(url).tell_one(1);
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
    // Initialize rtph264depay
    let rtph264depay = gst::ElementFactory::make("rtph264depay", Some("depay"))?;
    // Initialize queue 1
    let queue1 = gst::ElementFactory::make("queue", Some("queue1")).unwrap();
    // Initialize h264parse
    let h264parse = gst::ElementFactory::make("h264parse", None)?;
    // Initialize tee
    let tee = gst::ElementFactory::make("tee", Some("tee"))?;
    // Initialize queue 2
    let queue2 = gst::ElementFactory::make("queue", Some("queue2")).unwrap();
    // Initialize vaapih264dec
    let vaapih264dec = gst::ElementFactory::make("vaapih264dec", None)?;
    // Initialize videorate
    let videorate = gst::ElementFactory::make("videorate", Some("videorate"))?;
    // Initialize capsfilter for videorate
    let capsfilter = gst::ElementFactory::make("capsfilter", Some("filter"))?;
    // Initialize vaapipostproc
    let vaapipostproc = gst::ElementFactory::make("vaapipostproc", None)?;
    // Initialize vaapijpegenc
    let vaapijpegenc = gst::ElementFactory::make("vaapijpegenc", None)?;
    // Initialize appsink 1
    let sink1 = gst::ElementFactory::make("appsink", None)?;
    // Initialize vaapipostproc
    let vaapipostproc1 = gst::ElementFactory::make("vaapipostproc", None)?;
    // Initialize vaapijpegenc
    let vaapijpegenc1 = gst::ElementFactory::make("vaapijpegenc", None)?;
    // Initialize AppSink 2
    let sink2 = gst::ElementFactory::make("appsink", None)?;

    src.set_property("location", url);
    queue1.set_property_from_str("leaky", "downstream");
    queue2.set_property_from_str("leaky", "downstream");

    // FULLSCREEN
    sink1.set_property_from_str("name", "app1");
    sink1.set_property_from_str("max-buffers", "100");
    sink1.set_property_from_str("emit-signals", "false");
    sink1.set_property_from_str("drop", "true");

    // THUMNAIL
    vaapipostproc1.set_property_from_str("width", "720");
    vaapipostproc1.set_property_from_str("height", "480");
    sink2.set_property_from_str("name", "app2");
    sink2.set_property_from_str("max-buffers", "100");
    sink2.set_property_from_str("emit-signals", "false");
    sink2.set_property_from_str("drop", "true");

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
        &tee,
        &vaapipostproc,
        &vaapijpegenc,
        &sink1,
        &vaapipostproc1,
        &vaapijpegenc1,
        &sink2,
    ];
    pipeline.add_many(elements).expect("");

    let _ = src.link(&rtph264depay);
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
        src_pad.link(&sink_pad).expect("");
    });
    rtph264depay.link(&queue1)?;
    queue1.link(&h264parse)?;
    h264parse.link(&queue2)?;
    queue2.link(&vaapih264dec)?;
    vaapih264dec.link(&videorate)?;
    videorate.link(&capsfilter)?;
    capsfilter.link(&tee)?;

    tee.link(&vaapipostproc)?;
    vaapipostproc.link(&vaapijpegenc)?;
    vaapijpegenc.link(&sink1)?;

    tee.link(&vaapipostproc1)?;
    vaapipostproc1.link(&vaapijpegenc1)?;
    vaapijpegenc1.link(&sink2)?;

    let appsink1 = sink1
        .dynamic_cast::<gst_app::AppSink>()
        .expect("Sink element is expected to be an appsink!");

    let appsink2 = sink2
        .dynamic_cast::<gst_app::AppSink>()
        .expect("Sink element is expected to be an appsink!");

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

    let pipeline = set_framerate(pipeline, 5);

    Ok(pipeline)
}

fn main_loop(pipeline: gst::Pipeline) -> Result<(), Error> {
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
            MessageView::StateChanged(s) => {
                println!("{:?}", s.current());
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
