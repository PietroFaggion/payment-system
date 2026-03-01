package com.pietrofaggion.paymentsystem.controller;

import com.pietrofaggion.paymentsystem.dto.PaymentRequestDto;
import com.pietrofaggion.paymentsystem.dto.PaymentResponseDto;
import com.pietrofaggion.paymentsystem.exception.ApiError;
import com.pietrofaggion.paymentsystem.service.PaymentService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.security.Principal;

@Slf4j
@RestController
@RequestMapping("/payments")
@RequiredArgsConstructor
@Tag(name = "Payments", description = "Payment creation and retrieval")
public class PaymentController {

    private final PaymentService paymentService;

    /**
     * Validates the request, delegates the transfer to {@link com.pietrofaggion.paymentsystem.service.PaymentService},
     * and returns HTTP {@code 201 Created} with a {@code Location} header pointing to the new resource.
     * <p>
     * The endpoint is idempotent: sending the same {@code idempotencyKey} with identical parameters
     * returns the original transaction without re-executing the transfer. Reusing the key with
     * different parameters returns HTTP {@code 409 Conflict}.
     *
     * @param request validated payment details
     * @return {@code 201 Created} containing the transaction summary and a {@code Location} header
     */
    @PostMapping
    @Operation(summary = "Create a payment", description = "Transfers funds from sender to receiver. Idempotent: repeating the same idempotency key returns the original transaction.")
    @ApiResponses({
            @ApiResponse(responseCode = "201", description = "Payment created successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid request body", content = @Content(schema = @Schema(implementation = ApiError.class))),
            @ApiResponse(responseCode = "404", description = "Account not found", content = @Content(schema = @Schema(implementation = ApiError.class))),
            @ApiResponse(responseCode = "409", description = "Duplicate idempotency key or concurrent modification", content = @Content(schema = @Schema(implementation = ApiError.class))),
            @ApiResponse(responseCode = "422", description = "Insufficient funds or currency mismatch", content = @Content(schema = @Schema(implementation = ApiError.class)))
    })
    public ResponseEntity<PaymentResponseDto> createPayment(Principal principal,
                                                            @Valid @RequestBody PaymentRequestDto request) {
        log.info("POST /payments user={} sender={} receiver={} amount={} currency={} key={}",
                principal.getName(), request.getSenderAccountId(), request.getReceiverAccountId(),
                request.getAmount(), request.getCurrency(), request.getIdempotencyKey());

        PaymentResponseDto response = paymentService.createPayment(request);
        URI location = URI.create("/payments/" + response.getTransactionId());

        log.info("POST /payments completed user={} transactionId={} status={}",
                principal.getName(), response.getTransactionId(), response.getStatus());
        return ResponseEntity.created(location).body(response);
    }

    /**
     * Looks up a payment by its transaction ID and returns the full transaction details.
     *
     * @param transactionId the ID returned when the payment was created
     * @return {@code 200 OK} with the transaction, or {@code 404 Not Found} if it does not exist
     */
    @GetMapping("/{transactionId}")
    @Operation(summary = "Get payment by ID", description = "Retrieves a transaction by its ID.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Transaction found"),
            @ApiResponse(responseCode = "404", description = "Transaction not found", content = @Content(schema = @Schema(implementation = ApiError.class)))
    })
    public ResponseEntity<PaymentResponseDto> getPayment(Principal principal,
                                                         @PathVariable Long transactionId) {
        log.info("GET /payments/{} user={}", transactionId, principal.getName());
        return ResponseEntity.ok(paymentService.getPaymentById(transactionId));
    }
}
