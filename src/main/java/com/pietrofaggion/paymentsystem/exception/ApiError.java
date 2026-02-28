package com.pietrofaggion.paymentsystem.exception;

import java.time.OffsetDateTime;

public record ApiError(String message, OffsetDateTime timestamp) {
}
