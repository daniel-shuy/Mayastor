use std::ffi::{c_void, CString};

use once_cell::sync::Lazy;

use spdk_sys::{
    spdk_bdev_fn_table,
    spdk_bdev_io,
    spdk_bdev_io_type,
    spdk_get_io_channel,
    spdk_io_channel,
    spdk_json_write_array_end,
    spdk_json_write_ctx,
    spdk_json_write_named_array_begin,
    spdk_json_write_val_raw,
};

use crate::bdev::nexus::{
    instances,
    nexus_bdev::Nexus,
    nexus_channel::NexusChannel,
    nexus_io::{io_type, Bio},
};

static NEXUS_FN_TBL: Lazy<NexusFnTable> = Lazy::new(NexusFnTable::new);

pub struct NexusFnTable {
    pub(crate) f_tbl: spdk_bdev_fn_table,
}

unsafe impl Sync for NexusFnTable {}
unsafe impl Send for NexusFnTable {}

/// The FN table are function pointers called by SPDK when work is sent
/// our way. The functions are static, and shared between all instances.

impl NexusFnTable {
    fn new() -> Self {
        let mut f_tbl = spdk_bdev_fn_table::default();
        f_tbl.io_type_supported = Some(Self::io_supported);
        f_tbl.submit_request = Some(Self::io_submit);
        f_tbl.get_io_channel = Some(Self::io_channel);
        f_tbl.destruct = Some(Self::destruct);
        f_tbl.dump_info_json = Some(Self::dump_info_json);
        NexusFnTable {
            f_tbl,
        }
    }

    /// get a reference to this static function table to pass on to every
    /// instance
    pub fn table() -> &'static spdk_bdev_fn_table {
        &NEXUS_FN_TBL.f_tbl
    }

    /// check all the children for the specified IO type and return if it is
    /// supported
    extern "C" fn io_supported(
        ctx: *mut c_void,
        io_type: spdk_bdev_io_type,
    ) -> bool {
        let nexus = unsafe { Nexus::from_raw(ctx) };
        match io_type {
            // we always assume the device supports read/write commands
            // allow NVMe Admin as it is needed for local replicas
            io_type::READ | io_type::WRITE | io_type::NVME_ADMIN => true,
            io_type::FLUSH
            | io_type::RESET
            | io_type::UNMAP
            | io_type::WRITE_ZEROES => {
                let supported = nexus.io_is_supported(io_type);
                if !supported {
                    trace!(
                        "IO type {:?} not supported for {}",
                        io_type,
                        nexus.bdev.name()
                    );
                }
                supported
            }
            _ => {
                trace!(
                    "un matched IO type {} not supported for {}",
                    io_type,
                    nexus.bdev.name()
                );
                false
            }
        }
    }

    /// Main entry point to submit IO to the underlying children this uses
    /// callbacks rather than futures and closures for performance reasons.
    /// This function is not called when the IO is re-submitted (see below).
    pub extern "C" fn io_submit(
        channel: *mut spdk_io_channel,
        io: *mut spdk_bdev_io,
    ) {
        // only set the number of IO attempts before the first attempt
        let mut bio = Bio::from(io);
        bio.init();
        Self::io_submit_or_resubmit(channel, &mut bio);
    }

    /// Submit an IO to the children at the first or subsequent attempts.
    pub(crate) fn io_submit_or_resubmit(
        channel: *mut spdk_io_channel,
        nio: &mut Bio,
    ) {
        let mut ch = NexusChannel::inner_from_channel(channel);

        // set the fields that need to be (re)set per-attempt
        if nio.io_type() == io_type::READ {
            nio.reset(1);
        } else {
            nio.reset(ch.ch.len())
        }

        let nexus = nio.nexus_as_ref();
        let io_type = nio.io_type();
        match io_type {
            io_type::READ => nexus.readv(&nio, &mut ch),
            io_type::WRITE => nexus.writev(&nio, &ch),
            io_type::RESET => {
                trace!("{}: Dispatching RESET", nexus.bdev.name());
                nexus.reset(&nio, &ch)
            }
            io_type::UNMAP => {
                if nexus.io_is_supported(io_type) {
                    nexus.unmap(&nio, &ch)
                } else {
                    nio.fail();
                }
            }
            io_type::FLUSH => {
                // our replica's are attached to as nvme controllers
                // who always support flush. This can be troublesome
                // so we complete the IO directly.
                nio.reset(0);
                nio.ok();
            }
            io_type::WRITE_ZEROES => {
                if nexus.io_is_supported(io_type) {
                    nexus.write_zeroes(&nio, &ch)
                } else {
                    nio.fail()
                }
            }
            io_type::NVME_ADMIN => nexus.nvme_admin(&nio, &ch),
            _ => panic!(
                "{} Received unsupported IO! type {}",
                nexus.name, io_type
            ),
        };
    }

    /// called per core to create IO channels per Nexus instance
    extern "C" fn io_channel(ctx: *mut c_void) -> *mut spdk_io_channel {
        let n = unsafe { Nexus::from_raw(ctx) };
        trace!("{}: Get IO channel", n.bdev.name());
        unsafe { spdk_get_io_channel(ctx) }
    }

    /// called when the nexus instance is unregistered
    extern "C" fn destruct(ctx: *mut c_void) -> i32 {
        let nexus = unsafe { Nexus::from_raw(ctx) };
        nexus.destruct();
        let instances = instances();
        // removing the nexus from the list should cause a drop
        instances.retain(|x| x.name != nexus.name);

        0
    }

    /// device specific information which is returned
    /// by the get_bdevs RPC call.
    extern "C" fn dump_info_json(
        ctx: *mut c_void,
        w: *mut spdk_json_write_ctx,
    ) -> i32 {
        let nexus = unsafe { Nexus::from_raw(ctx) };
        unsafe {
            spdk_json_write_named_array_begin(
                w,
                "children\0".as_ptr() as *mut i8,
            );
        };

        let data =
            CString::new(serde_json::to_string(&nexus.children).unwrap())
                .unwrap();

        unsafe {
            spdk_json_write_val_raw(
                w,
                data.as_ptr() as *const _,
                data.as_bytes().len() as u64,
            );

            spdk_json_write_array_end(w);
        }
        0
    }
}
