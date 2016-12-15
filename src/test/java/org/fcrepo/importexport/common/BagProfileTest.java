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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author escowles
 * @since 2016-12-13
 */
public class BagProfileTest {

    private static final String FIELD1 = "field1";

    private static final String FIELD2 = "field2";

    private static final Map<String, Set<String>> rules = new HashMap<String, Set<String>>();

    final static File testFile = new File("src/test/resources/profiles/test.json");

    private LinkedHashMap<String, String> fields = new LinkedHashMap<String, String>();

    private BagProfile profile;

    static {
        final Set<String> set = new HashSet<>();
        set.add("value1");
        set.add("value2");
        set.add("value3");
        rules.put(FIELD1, set);
    }

    @Before
    public void setUp() throws Exception {
        profile = new BagProfile("test", new FileInputStream(testFile));
    }

    @Test
    public void testEnforceValues() throws ProfileValidationException {
        fields.put(FIELD1, "value1");
        BagProfile.validate("profile-section", rules, fields);
    }

    @Test(expected = ProfileValidationException.class)
    public void testEnforceValuesMissingRequired() throws ProfileValidationException {
        fields.put("field2", "value1");
        ProfileValidationUtil.validate("profile-section", rules, fields);
    }

    @Test(expected = ProfileValidationException.class)
    public void testEnforceValuesInvalidValue() throws ProfileValidationException {
        fields.put(FIELD1, "invalidValue");
        ProfileValidationUtil.validate("profile-section", rules, fields);
    }

    @Test
    public void testMultipleValidationErrorsInOneExceptionMessage() {
        fields.put(FIELD1, "invalidValue");
        rules.put(FIELD2, null);
        fields.put("field3", "any value");
        try {
            ProfileValidationUtil.validate("profile-section", rules, fields);
            Assert.fail("previous line should have failed.");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(FIELD1));
            Assert.assertTrue(e.getMessage().contains(FIELD2));
            Assert.assertFalse(e.getMessage().contains("field3"));
        }
    }

    @Test
    public void testFromFile() throws Exception {
        final File testFile = new File("src/test/resources/profiles/test.json");
        final BagProfile profile = new BagProfile("test", new FileInputStream(testFile));

        assertTrue(profile.getPayloadDigestAlgorithms().contains("md5"));
        assertTrue(profile.getPayloadDigestAlgorithms().contains("sha1"));
        assertTrue(profile.getPayloadDigestAlgorithms().contains("sha256"));

        assertFalse(profile.getTagDigestAlgorithms().contains("md5"));
        assertTrue(profile.getTagDigestAlgorithms().contains("sha1"));
        assertFalse(profile.getTagDigestAlgorithms().contains("sha256"));

        assertTrue(profile.getMetadataFields().keySet().contains("Source-Organization"));
        assertTrue(profile.getMetadataFields().keySet().contains("Organization-Address"));
        assertTrue(profile.getMetadataFields().keySet().contains("Contact-Name"));
        assertTrue(profile.getMetadataFields().keySet().contains("Contact-Phone"));
        assertTrue(profile.getMetadataFields().keySet().contains("Bag-Size"));
        assertTrue(profile.getMetadataFields().keySet().contains("Bagging-Date"));
        assertTrue(profile.getMetadataFields().keySet().contains("Payload-Oxum"));
        assertFalse(profile.getMetadataFields().keySet().contains("Contact-Email"));

        assertTrue(profile.getMetadataFields().keySet().contains("Title"));
        assertTrue(profile.getMetadataFields().keySet().contains("Access"));
        assertTrue(profile.getMetadataFields().get("Access").contains("Consortia"));
        assertTrue(profile.getMetadataFields().get("Access").contains("Institution"));
        assertTrue(profile.getMetadataFields().get("Access").contains("Restricted"));
    }

    public void testexport() {
        assertTrue( true );
    }
}
