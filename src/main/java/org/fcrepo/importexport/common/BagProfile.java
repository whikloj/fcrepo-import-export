/*
 * Licensed to DuraSpace under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * DuraSpace licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fcrepo.importexport.common;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author escowles
 * @since 2016-12-12
 */
public class BagProfile {

    private String profileName;
    private Set<String> payloadDigestAlgorithms;
    private Set<String> tagDigestAlgorithms;

    private Map<String, Set<String>> requiredFields = new HashMap<String, Set<String>>();

    private Map<String, Set<String>> metadataFields = new HashMap<String, Set<String>>();

    private Set<String> generatedFields = new HashSet<String>();
    // private Map<String, Set<String>> aptrustFields;

    /**
     * Default constructor.
     * @param in InputStream containing the Bag profile JSON document
     * @throws IOException when there is an I/O error reading JSON
     */
    public BagProfile(final String profileName, final InputStream in) throws IOException {
        this.profileName = profileName;
        final ObjectMapper mapper = new ObjectMapper();
        final JsonNode json = mapper.readTree(in);

        payloadDigestAlgorithms = arrayValues(json, "Manifests-Required");
        tagDigestAlgorithms = arrayValues(json, "Tag-Manifests-Required");
        if (tagDigestAlgorithms == null) {
            tagDigestAlgorithms = payloadDigestAlgorithms;
        }

        final String baseProfileName = json.get("baseProfile").asText();
        if (baseProfileName != null) {
            // Base Profiles are internal only for now.
            final URL url = this.getClass().getResource("/profiles/" + baseProfileName + ".json");
            if (url == null) {
                throw new IOException(String.format("Unable to access baseProfile \"%s\"", baseProfileName));
            }
            final BagProfile baseProfile = new BagProfile(baseProfileName, url.openStream());
            requiredFields = baseProfile.getRequiredFields();
            generatedFields = baseProfile.getGeneratedFields();
            metadataFields = baseProfile.getMetadataFields();
        }

        requiredFields.putAll(getRequiredFields(json));
        metadataFields.putAll(getMetadataFields(json));
        generatedFields.addAll(getGeneratedFields(json));

    }

    private static Set<String> arrayValues(final JsonNode json, final String key) {
        final JsonNode values = json.get(key);

        if (values == null) {
            return null;
        }

        final Set<String> results = new HashSet<>();
        for (int i = 0; i < values.size(); i++) {
            results.add(values.get(i).asText());
        }
        return results;
    }

    /**
     * Get a list of generated metadata fields, these can be subtracted from required to perform pre-export validation
     *
     * @param json the profile validation
     * @return the list
     */
    private static Set<String> getGeneratedFields(final JsonNode json) {
        return fieldGenerator(json, "generated").keySet().stream().collect(Collectors.toSet());
    }

    /**
     * Get all required metadata fields and return them with a set of valid values.
     *
     * @param json the profile JSON
     * @return the map
     */
    private static Map<String, Set<String>> getRequiredFields(final JsonNode json) {
        return fieldGenerator(json, "required");
    }

    /**
     * Get a map of all metadata fields and a set of valid values
     *
     * @param json the profile JSON
     * @return the map
     */
    private static Map<String, Set<String>> getMetadataFields(final JsonNode json) {
        return fieldGenerator(json, null);
    }

    /**
     * Returns a map of fields with a set of valid values (if any)
     *
     * @param json the original json
     * @param booleanKey a key to check for a true value or null for all fields
     * @return the map
     */
    private static Map<String, Set<String>> fieldGenerator(final JsonNode json, final String booleanKey) {
        final JsonNode fields = json.get("Metadata-Fields");

        if (fields == null) {
            return null;
        }

        final Map<String, Set<String>> results = new HashMap<>();
        for (final java.util.Iterator<String> it = fields.fieldNames(); it.hasNext(); ) {
            final String name = it.next();
            final JsonNode field = fields.get(name);
            if (booleanKey == null || (field.get(booleanKey) != null && field.get(booleanKey).asBoolean())) {
                results.put(name, arrayValues(field, "values"));
            }
        }

        return results;
    }


    /**
     * Get the required digest algorithms for payload manifests.
     * @return Set of digest algorithm names
     */
    public Set<String> getPayloadDigestAlgorithms() {
        return payloadDigestAlgorithms;
    }

    /**
     * Get the required digest algorithms for tag manifests.
     * @return Set of digest algorithm names
     */
    public Set<String> getTagDigestAlgorithms() {
        return tagDigestAlgorithms;
    }

    /**
     * Get the required Bag-Info metadata fields.
     * @return A map of field names to a Set of acceptable values (or null when the values are restricted).
     */
    public Map<String, Set<String>> getMetadataFields() {
        return metadataFields;
    }

    public Map<String, Set<String>> getRequiredFields() {
        return requiredFields;
    }

    public Set<String> getGeneratedFields() {
        return generatedFields;
    }

    /**
     * Get the required APTrust-Info metadata fields.
     * @return A map of field names to a Set of acceptable values (or null when the values are restricted),
     *    or null when no APTrust-Info fields are required.
     */


    /**
     * Validates the fields against the set of required fields and their constrained values.
     *
     * @param profileSection describes the section of the profile that is being validated.
     * @param requiredFields the required fields and any allowable values (if constrained).
     * @param fields The key value pairs to be validated.
     * @throws ProfileValidationException when the fields do not pass muster. The exception message contains a
     *         description of all validation errors found.
     */
    public void validate(final LinkedHashMap<String, String> fields) throws ProfileValidationException {

        if (metadataFields != null) {
            final StringBuilder errors = new StringBuilder();

            for (String fieldName : metadataFields.keySet()) {
                if (fields.containsKey(fieldName)) {
                    final String value = fields.get(fieldName);
                    final Set<String> validValues = metadataFields.get(fieldName);
                    if (validValues != null && !validValues.isEmpty()) {
                        if (!validValues.contains(value)) {
                            errors.append(String.format(
                                "\"%s\" is not valid for %s. Valid values are \"%s\"", profileName, value,
                                fieldName, validValues.stream().collect(Collectors.joining(","))));
                        }
                    }

                } else {
                    errors.append(String.format("\"%s\" is a required field.", profileName, fieldName));
                }
            }

            if (errors.length() > 0) {
                throw new ProfileValidationException(profileName, errors.toString());
            }
        }

    }

    /**
     * Get the profileName or filename
     *
     * @return the name or path to external file
     */
    public String getName() {
        return profileName;
    }
}
