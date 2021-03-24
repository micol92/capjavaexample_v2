package com.sap.cap.capbizservice.handlers;

import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Map;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sap.cds.ql.Select;
import com.sap.cds.ql.Update;
import com.sap.cds.ql.cqn.CqnSelect;
import com.sap.cds.ql.cqn.CqnUpdate;
import com.sap.cds.services.ErrorStatuses;
import com.sap.cds.services.ServiceException;
import com.sap.cds.services.cds.CdsService;
import com.sap.cds.services.handler.annotations.Before;
import com.sap.cds.services.persistence.PersistenceService;
import com.sap.cds.services.cds.CdsCreateEventContext;
import com.sap.cds.services.cds.CdsReadEventContext;

import com.sap.cds.services.handler.EventHandler;
import com.sap.cds.services.handler.annotations.On;
import com.sap.cds.services.handler.annotations.ServiceName;

import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;

import com.sap.cloud.sdk.service.prov.api.operations.Query;
import com.sap.cloud.sdk.service.prov.api.operations.Read;
import com.sap.cloud.sdk.service.prov.api.request.OrderByExpression;
import com.sap.cloud.sdk.service.prov.api.request.QueryRequest;
import com.sap.cloud.sdk.service.prov.api.request.ReadRequest;
import com.sap.cloud.sdk.service.prov.api.response.ErrorResponse;
import com.sap.cloud.sdk.service.prov.api.response.QueryResponse;
import com.sap.cloud.sdk.service.prov.api.response.ReadResponse;

import com.sap.cloud.sdk.s4hana.*;
import com.sap.cloud.sdk.cloudplatform.connectivity.*;

import cds.gen.cloud.sdk.capng.CapBusinessPartner;
import cds.gen.cloud.sdk.capng.CapBusinessPartner_;
import cds.gen.cloud.sdk.capng.Capng_;
import com.sap.cloud.sdk.s4hana.datamodel.odata.namespaces.businesspartner.*;

import com.sap.cloud.sdk.odatav2.connectivity.ODataException;
import com.sap.cloud.sdk.s4hana.datamodel.odata.namespaces.businesspartner.BusinessPartner;
import com.sap.cloud.sdk.s4hana.datamodel.odata.services.BusinessPartnerService;
import com.sap.cloud.sdk.s4hana.datamodel.odata.services.DefaultBusinessPartnerService;
import com.sap.cloud.sdk.s4hana.connectivity.DefaultErpHttpDestination;

import com.sap.cloud.sdk.s4hana.connectivity.exception.RequestExecutionException;
import com.sap.cloud.sdk.s4hana.connectivity.rfc.RfmRequest;
import com.sap.cloud.sdk.s4hana.connectivity.rfc.RfmRequestResult;

@Component
@ServiceName("cloud.sdk.capng")
public class BusinessPartnerReadListener implements EventHandler {

//    private final HttpDestination httpDestination = DestinationAccessor.getDestination("MYERPS20").asHttp();
    private final HttpDestination httpDestination = DestinationAccessor.getDestination("MYERPS20").asHttp().decorate(DefaultErpHttpDestination::new);

//    private static final Destination destinationRfc = DestinationAccessor.getDestination("MYRFCS20");

    @On(event = CdsService.EVENT_READ, entity = "cloud.sdk.capng.CapBusinessPartner")
    public void onRead(CdsReadEventContext context) throws ODataException {

        final Map<Object, Map<String, Object>> result = new HashMap<>();
   //     final List<BusinessPartner> businessPartners =
   //             new DefaultBusinessPartnerService().getAllBusinessPartner().top(10).execute(httpDestination);

               final List<BusinessPartner> businessPartners =
                    new DefaultBusinessPartnerService()
                            .getAllBusinessPartner()
                            .select(BusinessPartner.BUSINESS_PARTNER,
                                    BusinessPartner.LAST_NAME,
                                    BusinessPartner.FIRST_NAME,
                                    BusinessPartner.IS_MALE,
                                    BusinessPartner.IS_FEMALE,
                                    BusinessPartner.BUSINESS_PARTNER_CATEGORY,
                                    BusinessPartner.CREATION_DATE)
                            .top(200)
                            // TODO: uncomment the line below, if you are using the sandbox service
                            // .withHeader(APIKEY_HEADER, SANDBOX_APIKEY)
                            .executeRequest(httpDestination);

            System.out.printf(String.format("Found %d business partner(s).", businessPartners.size()));

        final List<CapBusinessPartner> capBusinessPartners =
                convertS4BusinessPartnersToCapBusinessPartners(businessPartners, "MYERPS20");
        capBusinessPartners.forEach(capBusinessPartner -> {
            result.put(capBusinessPartner.getId(), capBusinessPartner);
        });

        context.setResult(result.values());
/*
        try {
            final RfmRequestResult rfmTest = new RfmRequest("RFCPING", false) //false is for non-commit
                    .execute(destinationRfc);
            rfmTest.getSuccessMessages();
            System.out.println("rfc test result:"+rfmTest.getSuccessMessages());        

        } catch (RequestExecutionException e) {
            e.printStackTrace();
        }
*/        
    }

    @On(event = CdsService.EVENT_CREATE, entity = "cloud.sdk.capng.CapBusinessPartner")
    public void onCreate(CdsCreateEventContext context) throws ODataException {
        final BusinessPartnerService service = new DefaultBusinessPartnerService();

        Map<String, Object> m = context.getCqn().entries().get(0);
        //BusinessPartner bp = BusinessPartner.builder().firstName(m.get("firstName").toString()).lastName(m.get("surname").toString()).businessPartner(m.get("ID").toString()).businessPartnerCategory(m.get("bizPartnerCategory").toString()).build();
        BusinessPartner bp = BusinessPartner.builder().firstName(m.get("firstName").toString())
            .lastName(m.get("surname").toString())
            .businessPartner(m.get("ID").toString())
            .businessPartnerCategory(m.get("bizPartnerCategory").toString())
            .businessPartnerName("abc".toString())
            .organizationBPName1("abc".toString())
            .build();
        service.createBusinessPartner(bp).execute(httpDestination);

    }

    private List<CapBusinessPartner> convertS4BusinessPartnersToCapBusinessPartners(
            final List<BusinessPartner> s4BusinessPartners,
            final String destinationName) {
        final List<CapBusinessPartner> capBusinessPartners = new ArrayList<>();

        for (final BusinessPartner s4BusinessPartner : s4BusinessPartners) {
            final CapBusinessPartner capBusinessPartner = com.sap.cds.Struct.create(CapBusinessPartner.class);

            capBusinessPartner.setFirstName(s4BusinessPartner.getFirstName());
            capBusinessPartner.setSurname(s4BusinessPartner.getLastName());
            capBusinessPartner.setId(s4BusinessPartner.getBusinessPartner());
            capBusinessPartner.setBizPartnerCategory(s4BusinessPartner.getBusinessPartnerCategory());
            capBusinessPartner.setSourceDestination(destinationName);
            capBusinessPartners.add(capBusinessPartner);
        }


        return capBusinessPartners;
    }
}