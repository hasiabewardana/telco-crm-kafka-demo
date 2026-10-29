package lk.telco.crm.activation.api;

import lk.telco.crm.events.ActivationAction;
import lombok.Data;

import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.math.BigDecimal;

@Data
public class ActivationRequest {

    /** Sri Lankan mobile number in international format, e.g. 94771234567 */
    @NotBlank
    @Pattern(regexp = "947\\d{8}", message = "must be like 94771234567")
    private String msisdn;

    @NotBlank
    private String serviceCode;

    @NotNull
    private ActivationAction action;

    @NotNull
    @DecimalMin("0.00")
    private BigDecimal fee;

    @NotBlank
    private String channel;
}
