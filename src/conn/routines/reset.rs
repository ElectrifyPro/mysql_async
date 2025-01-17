use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use mysql_common::constants::Command;
#[cfg(feature = "tracing")]
use tracing::{debug_span, Instrument};

use crate::Conn;

use super::Routine;

/// A routine that performs `COM_RESET_CONNECTION`.
#[derive(Debug, Copy, Clone)]
pub struct ResetRoutine;

impl Routine<()> for ResetRoutine {
    fn call<'a>(&'a mut self, conn: &'a mut Conn) -> BoxFuture<'a, crate::Result<()>> {
        #[cfg(feature = "tracing")]
        let span = debug_span!("mysql_async::reset", mysql_async.connection.id = conn.id());

        let fut = async move {
            conn.write_command_data(Command::COM_RESET_CONNECTION, &[])
                .await?;
            conn.read_packet().await?;
            Ok(())
        };

        #[cfg(feature = "tracing")]
        let fut = fut.instrument(span);

        fut.boxed()
    }
}
