//! Resource abstraction (mirrors [`Tool`]): each [`Resource`] maps a family of `memory://`
//! URIs to a template + reader; [`ResourceRegistry`] dispatches across them.

use crate::protocol::{INTERNAL_ERROR, INVALID_PARAMS, RESOURCE_NOT_FOUND};
use crate::types::{ResourceContents, ResourceTemplate};

use super::tool::ToolCtx;

/// A resource-read failure, carrying the JSON-RPC error it maps to.
pub(super) enum ResourceError {
    NotFound(String),
    InvalidUri(String),
    Failed(String),
}

impl ResourceError {
    pub(super) fn code(&self) -> i64 {
        match self {
            ResourceError::NotFound(_) => RESOURCE_NOT_FOUND,
            ResourceError::InvalidUri(_) => INVALID_PARAMS,
            ResourceError::Failed(_) => INTERNAL_ERROR,
        }
    }

    pub(super) fn message(self) -> String {
        match self {
            ResourceError::NotFound(m)
            | ResourceError::InvalidUri(m)
            | ResourceError::Failed(m) => m,
        }
    }
}

/// One family of readable resources (e.g. atoms by id).
pub(super) trait Resource: Send + Sync {
    fn template(&self) -> ResourceTemplate;

    /// `Ok(None)` means "not my URI - let the next resource try".
    fn read(
        &self,
        ctx: &ToolCtx,
        uri: &str,
    ) -> Result<Option<Vec<ResourceContents>>, ResourceError>;
}

/// The set of resource families.
pub(super) struct ResourceRegistry {
    resources: Vec<Box<dyn Resource>>,
}

impl ResourceRegistry {
    pub(super) fn new(resources: Vec<Box<dyn Resource>>) -> Self {
        Self { resources }
    }

    pub(super) fn templates(&self) -> Vec<ResourceTemplate> {
        self.resources.iter().map(|r| r.template()).collect()
    }

    /// First resource that recognizes `uri` wins.
    pub(super) fn read(
        &self,
        ctx: &ToolCtx,
        uri: &str,
    ) -> Result<Vec<ResourceContents>, ResourceError> {
        for resource in &self.resources {
            if let Some(contents) = resource.read(ctx, uri)? {
                return Ok(contents);
            }
        }
        Err(ResourceError::InvalidUri(format!(
            "no resource handles uri: {uri}"
        )))
    }
}
