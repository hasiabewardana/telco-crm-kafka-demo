package lk.telco.crm.activation.api;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

@RestControllerAdvice
public class ApiExceptionHandler {

    /** Kafka did not confirm the write: tell the caller to retry later instead of pretending success. */
    @ExceptionHandler(EventPublishException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Map<String, String> publishFailed(EventPublishException e) {
        Map<String, String> body = new LinkedHashMap<>();
        body.put("eventId", e.getEventId());
        body.put("status", "NOT_PUBLISHED");
        body.put("error", e.getCause().getClass().getSimpleName() + ": " + e.getCause().getMessage());
        return body;
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> invalid(MethodArgumentNotValidException e) {
        Map<String, String> errors = new LinkedHashMap<>();
        e.getBindingResult().getFieldErrors()
                .forEach(fe -> errors.put(fe.getField(), fe.getDefaultMessage()));
        return errors;
    }
}
