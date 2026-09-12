//! Library half of this crate: just the pieces `logger` (a separate demo
//! binary that talks to syncd's encrypted endpoints on the test rig's
//! behalf, see src/bin/logger.rs) needs to share with the `syncd` binary
//! itself. Everything else (core, discovery, persist, telemetry) stays
//! private to main.rs, since nothing outside it needs them.

pub mod crypto;
