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
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedList;
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
//        addPatients(Paths.get(resourceDirectory.toString(), "patients.tsv"));
//        addEncounters(Paths.get(resourceDirectory.toString(), "encounters.tsv"));
//        addObservations(Paths.get(resourceDirectory.toString(), "observations.tsv"));

//        addPatients(Paths.get(resourceDirectory.toString(), "patients.tsv"), 500);
//        addEncounters(Paths.get(resourceDirectory.toString(), "encounters.tsv"), 500);
//        addObservations(Paths.get(resourceDirectory.toString(), "observations.tsv"), 500);
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

    public void addObservations(Path tsvFile, int batchSize) {
        Map<String, Patient> patientReferences = new HashMap<>();
        Map<String, Encounter> encounterReferences = new HashMap<>();
        List<String> batch = new LinkedList<>();
        try (BufferedReader reader = Files.newBufferedReader(tsvFile, Charset.defaultCharset())) {
            reader.readLine(); // skip header
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (batch.size() == batchSize) {
                    addObservation(batch, patientReferences, encounterReferences);
                    batch.clear();
                }

                batch.add(line);
            }
        } catch (IOException | ParseException exception) {
            exception.printStackTrace(System.err);
        }

        try {
            addObservation(batch, patientReferences, encounterReferences);
            batch.clear();
        } catch (ParseException exception) {
            exception.printStackTrace(System.err);
        }
    }

    public void addEncounters(Path tsvFile, int batchSize) {
        Map<String, Patient> patientReferences = new HashMap<>();
        List<String> batch = new LinkedList<>();
        try (BufferedReader reader = Files.newBufferedReader(tsvFile, Charset.defaultCharset())) {
            reader.readLine(); // skip header
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (batch.size() == batchSize) {
                    addEncounter(batch, patientReferences);
                    batch.clear();
                }

                batch.add(line);
            }
        } catch (IOException | ParseException exception) {
            exception.printStackTrace(System.err);
        }

        try {
            addEncounter(batch, patientReferences);
            batch.clear();
        } catch (ParseException exception) {
            exception.printStackTrace(System.err);
        }
    }

    private void addObservation(
            List<String> batch,
            Map<String, Patient> patientReferences,
            Map<String, Encounter> encounterReferences) throws ParseException {
        List<Resource> resources = new LinkedList<>();
        for (String line : batch) {
            Observation observation = ObservationResourceMapper.getObservation(Delimiters.TAB_DELIM.split(line));

            Patient patient = patientReferences.get(observation.getSubject().getReference());
            if (patient == null) {
                Resource resource = findPatient(observation.getSubject()).getEntryFirstRep().getResource();
                if (resource != null) {
                    patient = (Patient) resource;
                    patientReferences.put(observation.getSubject().getReference(), patient);
                }
            }
            Encounter encounter = encounterReferences.get(observation.getEncounter().getReference());
            if (encounter == null) {
                Resource resource = findEncounter(observation.getEncounter()).getEntryFirstRep().getResource();
                if (resource != null) {
                    encounter = (Encounter) resource;
                    encounterReferences.put(observation.getEncounter().getReference(), encounter);
                }
            }

            if (!(patient == null || encounter == null)) {
                observation.setSubject(new Reference()
                        .setReference("Patient/" + patient.getIdElement().getIdPart())
                        .setDisplay(patient.getNameFirstRep().getNameAsSingleString()));
                observation.setEncounter(new Reference()
                        .setReference("Encounter/" + encounter.getIdElement().getIdPart()));
            }

            resources.add(observation);
        }

        addResources(resources, "Observation");
    }

    private void addEncounter(List<String> batch, Map<String, Patient> patientReferences) throws ParseException {
        List<Resource> resources = new LinkedList<>();
        for (String line : batch) {
            Encounter encounter = EncounterResourceMapper.getEncounter(Delimiters.TAB_DELIM.split(line));

            Patient patient = patientReferences.get(encounter.getSubject().getReference());
            if (patient == null) {
                Resource resource = findPatient(encounter.getSubject()).getEntryFirstRep().getResource();
                if (resource != null) {
                    patient = (Patient) resource;
                    patientReferences.put(encounter.getSubject().getReference(), patient);
                }
            }
            if (patient != null) {
                encounter.setSubject(new Reference()
                        .setReference("Patient/" + patient.getIdElement().getIdPart())
                        .setDisplay(patient.getNameFirstRep().getNameAsSingleString()));
            }

            resources.add(encounter);
        }

        addResources(resources, "Encounter");
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

    public void addPatients(Path tsvFile, int batchSize) {
        List<String> batch = new LinkedList<>();
        try (BufferedReader reader = Files.newBufferedReader(tsvFile, Charset.defaultCharset())) {
            reader.readLine(); // skip header
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (batch.size() == batchSize) {
                    addPatients(batch);
                    batch.clear();
                }

                batch.add(line);
            }
        } catch (IOException | ParseException exception) {
            exception.printStackTrace(System.err);
        }

        try {
            addPatients(batch);
            batch.clear();
        } catch (ParseException exception) {
            exception.printStackTrace(System.err);
        }
    }

    private void addPatients(List<String> batch) throws ParseException {
        List<Resource> resources = new LinkedList<>();
        for (String line : batch) {
            resources.add(PatientResourceMapper.getPatient(Delimiters.TAB_DELIM.split(line)));
        }

        addResources(resources, "Patient");
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
