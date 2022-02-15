use std::{io::BufWriter, num::NonZeroU32};

use bastion::prelude::*;
use anyhow::Error;
use byte_slice_cast::*;
use derive_more::{Display, Error};
use fast_image_resize as fr;
use gst::{
    element_error, glib,
    prelude::{Cast, ElementExt, GstBinExt, GstObjectExt},
};
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
        "rtsp://10.50.31.172/1/h264major",
        "rtsp://10.50.13.231/1/h264major",
        "rtsp://10.50.13.233/1/h264major",
        "rtsp://10.50.13.234/1/h264major",
        "rtsp://10.50.13.235/1/h264major",
        "rtsp://10.50.13.236/1/h264major",
        "rtsp://10.50.13.237/1/h264major",
        "rtsp://10.50.13.240/1/h264major",
        "rtsp://10.50.13.241/1/h264major",
        "rtsp://10.50.13.243/1/h264major",
        "rtsp://10.50.13.244/1/h264major",
        "rtsp://10.50.13.245/1/h264major",
        "rtsp://10.50.13.248/1/h264major",
        "rtsp://10.50.13.249/1/h264major",
    ];
    Bastion::init();
    Bastion::start();

    for url in cam_list {
        blocking!(
            create_pipeline(url).and_then(|pipeline| main_loop(pipeline, url))
        );
    }

    Bastion::block_until_stopped();
}

fn create_pipeline(url: &str) -> Result<gst::Pipeline, Error> {
    println!("Pipeline {}", url);
    gst::init()?;

    let pipeline = gst::parse_launch(&format!(
        "rtspsrc location={} !
        rtph264depay ! queue leaky=2 !
        h264parse ! queue leaky=2 !
        vaapih264dec ! videorate ! video/x-raw,framerate=5/1 !
        vaapipostproc ! vaapijpegenc !
        appsink name=sink max-buffers=100 emit-signals=false drop=true",
        url
    ))?
    .downcast::<gst::Pipeline>()
    .expect("Expected Gst Pipeline");

    let appsink = pipeline
        .by_name("sink")
        .expect("Sink element not found")
        .downcast::<gst_app::AppSink>()
        .expect("Sink element was expected to be an appsink");

    appsink.set_callbacks(
        gst_app::AppSinkCallbacks::builder()
            .new_sample(move |appsink| {
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

                let image = image::load_from_memory_with_format(samples, ImageFormat::Jpeg);
                let new_image = match image {
                    Ok(image) => {
                        let width = NonZeroU32::new(image.width()).unwrap();
                        let height = NonZeroU32::new(image.height()).unwrap();

                        let mut src_image = fr::Image::from_vec_u8(
                            width,
                            height,
                            image.to_rgb8().into_raw(),
                            fr::PixelType::U8x3,
                        )
                        .unwrap();

                        let dst_width = NonZeroU32::new(720).unwrap();
                        let dst_height = NonZeroU32::new(480).unwrap();

                        let mut dst_image =
                            fr::Image::new(dst_width, dst_height, src_image.pixel_type());

                        let mut dst_view = dst_image.view_mut();

                        let mut resizer =
                            fr::Resizer::new(fr::ResizeAlg::Convolution(fr::FilterType::Box));

                        resizer.resize(&src_image.view(), &mut dst_view).unwrap();

                        let mut result_buf = BufWriter::new(Vec::new());
                        image::codecs::jpeg::JpegEncoder::new(&mut result_buf)
                            .encode(
                                dst_image.buffer(),
                                dst_width.get(),
                                dst_height.get(),
                                ColorType::Rgb8,
                            )
                            .unwrap();

                        Vec::from(result_buf.into_inner().unwrap())
                    }
                    Err(_) => unreachable!(),
                };

                Ok(gst::FlowSuccess::Ok)
            })
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
