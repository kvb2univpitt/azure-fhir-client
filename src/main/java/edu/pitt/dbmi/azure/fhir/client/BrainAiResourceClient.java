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
import ca.uhn.fhir.rest.client.api.IGenericClient;
import edu.pitt.dbmi.fhir.resource.mapper.r4.brainai.EncounterResourceMapper;
import edu.pitt.dbmi.fhir.resource.mapper.r4.brainai.ObservationResourceMapper;
import edu.pitt.dbmi.fhir.resource.mapper.r4.brainai.PatientResourceMapper;
import edu.pitt.dbmi.fhir.resource.mapper.util.Delimiters;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;

/**
 *
 * Jun 21, 2022 10:40:54 AM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class BrainAiResourceClient extends AbstractResourceClient {

    public BrainAiResourceClient(IGenericClient client) {
        super(client);
    }

    public void addResources(Path resourceDirectory) throws IOException {
        addPatients(Paths.get(resourceDirectory.toString(), "patients.tsv"));
        addEncounters(Paths.get(resourceDirectory.toString(), "encounters.tsv"));
        addObservations(Paths.get(resourceDirectory.toString(), "observations.tsv"));
    }

    public Bundle addObservations(Path tsvFile) {
        List<Observation> observations = ObservationResourceMapper.getObservations(tsvFile, Delimiters.TAB_DELIM);
        Map<String, Patient> patientReferences = fetchPatientsFromObservations(observations);
        Map<String, Encounter> encounterReferences = fetchEncountersFromObservations(observations);

        List<Resource> resources = observations.stream()
                .map(observation -> {
                    Patient patient = patientReferences.get(observation.getSubject().getReference());
                    Encounter encounter = encounterReferences.get(observation.getEncounter().getReference());
                    if (!(patient == null || encounter == null)) {
                        observation.setSubject(new Reference()
                                .setReference("Patient/" + patient.getIdElement().getIdPart())
                                .setDisplay(patient.getNameFirstRep().getNameAsSingleString()));
                        observation.setEncounter(new Reference()
                                .setReference("Encounter/" + encounter.getIdElement().getIdPart()));
                    }
                    return (Resource) observation;
                })
                .collect(Collectors.toList());

        return addResources(resources, "Observation");
    }

    public Bundle addEncounters(Path tsvFile) {
        List<Encounter> encounters = EncounterResourceMapper.getEncounters(tsvFile, Delimiters.TAB_DELIM);
        Map<String, Patient> patientReferences = fetchPatientsFromEncounters(encounters);

        List<Resource> resources = encounters.stream()
                .map(encounter -> {
                    Patient patient = patientReferences.get(encounter.getSubject().getReference());
                    if (patient != null) {
                        encounter.setSubject(new Reference()
                                .setReference("Patient/" + patient.getIdElement().getIdPart())
                                .setDisplay(patient.getNameFirstRep().getNameAsSingleString()));
                    }

                    return (Resource) encounter;
                })
                .collect(Collectors.toList());

        return addResources(resources, "Encounter");
    }

    public Bundle addPatients(Path tsvFile) {
        List<Resource> patients = PatientResourceMapper
                .getPatients(tsvFile, Delimiters.TAB_DELIM).stream()
                .map(e -> (Resource) e).collect(Collectors.toList());

        return addResources(patients, "Patient");
    }

    private Map<String, Encounter> fetchEncountersFromObservations(List<Observation> observations) {
        Map<String, Encounter> references = new HashMap<>();

        observations.stream()
                .map(observation -> observation.getEncounter())
                .forEach(subject -> {
                    String reference = subject.getReference();
                    if (!references.containsKey(reference)) {
                        Resource resource = findEncounter(subject).getEntryFirstRep().getResource();
                        if (resource != null) {
                            references.put(reference, (Encounter) resource);
                        }
                    }
                });

        return references;
    }

    private Map<String, Patient> fetchPatientsFromObservations(List<Observation> observations) {
        Map<String, Patient> references = new HashMap<>();

        observations.stream()
                .map(observation -> observation.getSubject())
                .forEach(subject -> {
                    String reference = subject.getReference();
                    if (!references.containsKey(reference)) {
                        Resource resource = findPatient(subject).getEntryFirstRep().getResource();
                        if (resource != null) {
                            references.put(reference, (Patient) resource);
                        }
                    }
                });

        return references;
    }

    private Map<String, Patient> fetchPatientsFromEncounters(List<Encounter> encounters) {
        Map<String, Patient> references = new HashMap<>();

        encounters.stream()
                .map(encounter -> encounter.getSubject())
                .forEach(subject -> {
                    String reference = subject.getReference();
                    if (!references.containsKey(reference)) {
                        Resource resource = findPatient(subject).getEntryFirstRep().getResource();
                        if (resource != null) {
                            references.put(reference, (Patient) resource);
                        }
                    }
                });

        return references;
    }

    public Bundle findEncounter(Reference subject) {
        return client
                .search()
                .forResource(Encounter.class)
                .where(Patient.IDENTIFIER.exactly().systemAndValues("urn:oid:2.16.840.1.113883.3.552", subject.getReference()))
                .returnBundle(Bundle.class)
                .cacheControl(new CacheControlDirective().setNoCache(true))
                .execute();
    }

    public Bundle findPatient(Reference subject) {
        return client
                .search()
                .forResource(Patient.class)
                .where(Patient.IDENTIFIER.exactly().systemAndValues("urn:oid:2.16.840.1.113883.3.552", subject.getReference()))
                .returnBundle(Bundle.class)
                .cacheControl(new CacheControlDirective().setNoCache(true))
                .execute();
    }

}
