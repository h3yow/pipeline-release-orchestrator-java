package com.github.h3yow.pipelinereleaseorchestratorjava.model;

/** Release record status: ACTIVE until superseded or rolled back. */
public enum ReleaseStatus {
    ACTIVE,
    SUPERSEDED,
    ROLLED_BACK
}
