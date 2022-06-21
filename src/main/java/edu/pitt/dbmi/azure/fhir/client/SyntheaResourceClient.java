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

import ca.uhn.fhir.rest.api.CacheControlDirective;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import edu.pitt.dbmi.fhir.resource.mapper.util.JsonResourceConverterR4;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;

/**
 *
 * Jun 20, 2022 5:56:45 PM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
public class SyntheaResourceClient extends AbstractResourceClient {

    private final IGenericClient client;

    public SyntheaResourceClient(IGenericClient client) {
        this.client = client;
    }

    public void deleteAllResourceBundle(Path bundleFile) throws IOException {
        Map<Class, Resource> resourceClasses = new HashMap<>();
        try ( BufferedReader reader = Files.newBufferedReader(bundleFile, Charset.defaultCharset())) {
            Bundle bundle = (Bundle) JsonResourceConverterR4.parseResource(reader);
            bundle.getEntry().forEach(e -> resourceClasses.put(e.getResource().getClass(), e.getResource()));
        }

        resourceClasses.values()
                .forEach(resource -> {
                    Bundle searchBundle = client
                            .search()
                            .forResource(resource.getClass())
                            .returnBundle(Bundle.class)
                            .cacheControl(new CacheControlDirective().setNoCache(true))
                            .execute();

                    deleteResources(searchBundle, client);
                });

    }

    public Bundle addAllResourceBundle(Path bundleFile) throws IOException {
        try ( BufferedReader reader = Files.newBufferedReader(bundleFile, Charset.defaultCharset())) {
            Bundle bundle = (Bundle) JsonResourceConverterR4.parseResource(reader);

            return client.transaction().withBundle(bundle).execute();
        }
    }

    public void addResourceBundle(Path bundleFile) throws IOException {
        List<Patient> patients = getPatients(bundleFile);
        List<Encounter> encounters = getEncounters(bundleFile);
        List<Observation> observations = getObservations(bundleFile);

        patients.forEach(patient -> {
            MethodOutcome patientOutcome = addResource(patient, client);
            final Patient uploadedPatient = (Patient) patientOutcome.getResource();

            encounters.stream()
                    .filter(encounter -> patient.getIdElement().getIdPart().equals(encounter.getSubject().getReference()))
                    .forEach(encounter -> {
                        encounter.setSubject(new Reference()
                                .setReference("Patient/" + uploadedPatient.getIdElement().getIdPart())
                                .setDisplay(uploadedPatient.getNameFirstRep().getNameAsSingleString()));

                        MethodOutcome encounterOutcome = addResource(encounter, client);
                        final Encounter uploadedEncounter = (Encounter) encounterOutcome.getResource();

                        final Bundle bundle = new Bundle();
                        bundle.setType(Bundle.BundleType.TRANSACTION);
                        observations.stream()
                                .filter(observation -> patient.getIdElement().getIdPart().equals(observation.getSubject().getReference()) && encounter.getIdElement().getIdPart().equals(observation.getEncounter().getReference()))
                                .forEach(observation -> {
                                    observation.setSubject(new Reference()
                                            .setReference("Patient/" + uploadedPatient.getIdElement().getIdPart())
                                            .setDisplay(uploadedPatient.getNameFirstRep().getNameAsSingleString()));
                                    observation.setEncounter(new Reference()
                                            .setReference("Encounter/" + uploadedEncounter.getIdElement().getIdPart()));

                                    bundle.addEntry()
                                            .setResource(observation)
                                            .getRequest()
                                            .setUrl("Observation")
                                            .setMethod(Bundle.HTTPVerb.POST);
                                });

                        Bundle observationBundle = client.transaction().withBundle(bundle).execute();
                    });

        });

        System.out.printf("Patients: %d%n", patients.size());
        System.out.printf("Encounters: %d%n", encounters.size());
        System.out.printf("Observations: %d%n", observations.size());
    }

    public Bundle addEncounters(List<Encounter> encounters, Patient patient) {
        Bundle bundle = new Bundle();
        bundle.setType(Bundle.BundleType.TRANSACTION);

        encounters.forEach(encounter -> {
            encounter.setSubject(new Reference()
                    .setReference("Patient/" + patient.getIdElement().getIdPart())
                    .setDisplay(patient.getNameFirstRep().getNameAsSingleString()));

            bundle.addEntry()
                    .setResource(encounter)
                    .getRequest()
                    .setUrl("Encounter")
                    .setMethod(Bundle.HTTPVerb.POST);
        });

        return client.transaction().withBundle(bundle).execute();
    }

    public List<Observation> getObservations(Path bundleFile) throws IOException {
        List<Observation> observations = new LinkedList<>();

        try ( BufferedReader reader = Files.newBufferedReader(bundleFile, Charset.defaultCharset())) {
            Bundle bundle = (Bundle) JsonResourceConverterR4.parseResource(reader);
            bundle.getEntry().stream()
                    .filter(e -> e.getResource().fhirType().equals("Observation"))
                    .map(e -> (Observation) e.getResource())
                    .forEach(observations::add);
        }

        return observations;
    }

    public List<Encounter> getEncounters(Path bundleFile) throws IOException {
        List<Encounter> encounters = new LinkedList<>();

        try ( BufferedReader reader = Files.newBufferedReader(bundleFile, Charset.defaultCharset())) {
            Bundle bundle = (Bundle) JsonResourceConverterR4.parseResource(reader);
            bundle.getEntry().stream()
                    .filter(e -> e.getResource().fhirType().equals("Encounter"))
                    .map(e -> (Encounter) e.getResource())
                    .forEach(encounters::add);
        }

        return encounters;
    }

    public List<Patient> getPatients(Path bundleFile) throws IOException {
        List<Patient> patients = new LinkedList<>();

        try ( BufferedReader reader = Files.newBufferedReader(bundleFile, Charset.defaultCharset())) {
            Bundle bundle = (Bundle) JsonResourceConverterR4.parseResource(reader);
            bundle.getEntry().stream()
                    .filter(e -> e.getResource().fhirType().equals("Patient"))
                    .map(e -> (Patient) e.getResource())
                    .forEach(patients::add);
        }

        return patients;
    }

}
