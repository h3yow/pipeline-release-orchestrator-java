package com.github.h3yow.pipelinereleaseorchestratorjava.web.dto;

import com.github.h3yow.pipelinereleaseorchestratorjava.model.RejectReason;

/** Distinguishable error body carrying the machine-readable reason code. */
public record ErrorResponse(RejectReason reason, String message) {
}
