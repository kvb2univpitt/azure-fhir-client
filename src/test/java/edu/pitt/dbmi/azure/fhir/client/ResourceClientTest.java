/*
 * Copyright (C) 2022 University of Pittsburgh.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */
package edu.pitt.dbmi.azure.fhir.client;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BearerTokenAuthInterceptor;
import edu.pitt.dbmi.fhir.resource.mapper.util.JsonResourceConverterR4;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.Test;

/**
 *
 * Jun 19, 2022 9:43:30 PM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
public class ResourceClientTest {

    private final FhirContext fhirContext = FhirContext.forR4();

    private final String fhirUrl = "https://brainai-init.fhir.azurehealthcareapis.com";
    private final String accessToken = "";

    @Test
    public void test() {
        IGenericClient client = getClient();
        PatientResourceClient patientClient = new PatientResourceClient(client);
        EncounterResourceClient encounterClient = new EncounterResourceClient(client);
        SyntheaResourceClient syntheaClient = new SyntheaResourceClient(client);
        BrainAiResourceClient brainAiClient = new BrainAiResourceClient(client);
        try {
//            addBrainAiResources(brainAiClient);

//            deleteSyntheaData(syntheaClient);
//            fetchPatientById(patientClient);
//            deletePatients(patientClient);
//            deleteEncounters(encounterClient);
//            loadSyntheaData(syntheaClient);
//            uploadSyntheaEncountersFromFile(encounterClient);
//            uploadSyntheaPatientsFromFile(patientClient);
//            deleteEncounters(encounterClient);
//            deletePatients(patientClient);
//            fetchEncounters(encounterClient);
//            fetchPatients(patientClient);
        } catch (Exception exception) {
            exception.printStackTrace(System.err);
        }
    }

    private void addBrainAiResources(BrainAiResourceClient brainAiClient) throws IOException {
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("Brain AI Synthea");
        System.out.println("--------------------------------------------------------------------------------");

        Path resourceDirectory = Paths.get(ResourceClientTest.class.getResource("/data/brainai").getFile());
        brainAiClient.addResources(resourceDirectory);

        System.out.println();
        System.out.println();
    }

    private void deleteSyntheaData(SyntheaResourceClient syntheaClient) throws IOException {
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("Delete Synthea");
        System.out.println("--------------------------------------------------------------------------------");

        Path file = Paths.get(ResourceClientTest.class.getResource("/data/synthea/Aaron697_Brekke496_2fa15bc7-8866-461a-9000-f739e425860a.json").getFile());
        syntheaClient.deleteAllResourceBundle(file);

        System.out.println();
        System.out.println();
    }

    private void loadSyntheaData(SyntheaResourceClient syntheaClient) throws IOException {
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("Load Synthea");
        System.out.println("--------------------------------------------------------------------------------");

        Path file = Paths.get(ResourceClientTest.class.getResource("/data/synthea/Aaron697_Brekke496_2fa15bc7-8866-461a-9000-f739e425860a.json").getFile());
        syntheaClient.addResourceBundle(file);

        System.out.println();
        System.out.println();
    }

    private void deleteEncounters(EncounterResourceClient client) {
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("Delete Encounters");
        System.out.println("--------------------------------------------------------------------------------");

        Bundle bundle = client.deleteEncounters();
        if (bundle != null) {
            System.out.println(JsonResourceConverterR4.resourceToJson(bundle));
        }

        System.out.println();
        System.out.println();
    }

    private void deletePatients(PatientResourceClient client) {
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("Delete Patients");
        System.out.println("--------------------------------------------------------------------------------");

        Bundle bundle = client.deletePatients();
        if (bundle != null) {
            System.out.println(JsonResourceConverterR4.resourceToJson(bundle));
        }

        System.out.println();
        System.out.println();
    }

    private void fetchEncounters(EncounterResourceClient client) {
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("Print Each Encounter");
        System.out.println("--------------------------------------------------------------------------------");

        Bundle bundle = client.getEncounters();
        Encounter[] encounters = bundle.getEntry().stream()
                .map(e -> (Encounter) e.getResource())
                .collect(Collectors.toList()).toArray(Encounter[]::new);
        for (int i = 0; i < encounters.length; i++) {
            Encounter encounter = encounters[i];
            System.out.printf("%n---------   Encounter %d (%s)   --------%n", i + 1, encounter.getTypeFirstRep().getText());
            System.out.println(JsonResourceConverterR4.resourceToJson(encounter));
        }

        System.out.println();
        System.out.println();
    }

    private void fetchPatientById(PatientResourceClient client) {
        String id = "f83dd9ca-ed11-48ae-8369-62f02d271e83";
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf("Fetch Patient By ID: %s%n", id);
        System.out.println("--------------------------------------------------------------------------------");

        Patient patient = client.getPatient(id);
        System.out.println(patient.getIdElement().getIdPart());
//        System.out.println(JsonResourceConverterR4.resourceToJson(patient));

        System.out.println();
        System.out.println();
    }

    private void fetchPatients(PatientResourceClient client) {
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("Print Each Patient");
        System.out.println("--------------------------------------------------------------------------------");

        Bundle bundle = client.getPatients();
        Patient[] patients = bundle.getEntry().stream()
                .map(e -> (Patient) e.getResource())
                .collect(Collectors.toList()).toArray(Patient[]::new);
        for (int i = 0; i < patients.length; i++) {
            Patient patient = patients[i];
            System.out.printf("%n---------   Patient %d (%s)   --------%n", i + 1, patient.getNameFirstRep().getNameAsSingleString());
            System.out.println(JsonResourceConverterR4.resourceToJson(patient));
        }

        System.out.println();
        System.out.println();
    }

    private IGenericClient getClient() {
        fhirContext.getRestfulClientFactory().setSocketTimeout(200 * 1000);

        IGenericClient client = fhirContext.newRestfulGenericClient(fhirUrl);
        client.registerInterceptor(new BearerTokenAuthInterceptor(accessToken));

        return client;
    }

}
