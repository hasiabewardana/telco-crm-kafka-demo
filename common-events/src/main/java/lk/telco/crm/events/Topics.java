package lk.telco.crm.events;

/**
 * Single source of truth for topic names, shared by all producers and consumers.
 */
public final class Topics {

    public static final String SERVICE_ACTIVATION = "telco.service-activation";
    public static final String BILLING = "telco.billing";

    private Topics() {
    }
}
