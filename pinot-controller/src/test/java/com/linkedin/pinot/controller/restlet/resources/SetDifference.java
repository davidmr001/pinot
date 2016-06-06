/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.controller.restlet.resources;

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SetDifference {
  private static Logger LOGGER = LoggerFactory.getLogger(SetDifference.class);
  public static void main(String[] args) {
    Set<String> s = new HashSet<>();
    s.add("a");
    s.add("b");
    Set<String> b = new HashSet<>();
    b.add("a");
    b.add("b");
    //b.add("c");
    Sets.SetView<String> difference = Sets.difference(s, b);
    if (difference == null) {
      System.out.println("null");
    } else {
      System.out.println("not null" +  difference.size());
    }

    Map<String, String> m1 = new HashMap<>();
    m1.put("a","x");
    m1.put("b", "y");

    Map<String, String> m2 = new HashMap<>();
    m2.put("a","x");
    m2.put("b", "y");
    //m2.put("b", "y");
    Sets.SetView<String> difference2 = Sets.difference(m1.keySet(), m2.keySet());
    if (difference2 == null) {
      System.out.println("null");
    } else {
      System.out.println("not null" +  difference2.size());
    }


  }
}
