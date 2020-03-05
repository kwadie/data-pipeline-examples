/*
 *
 *  * Copyright (C) 2020 Google Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  * use this file except in compliance with the License. You may obtain a copy of
 *  * the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  * License for the specific language governing permissions and limitations under
 *  * the License.
 *
 */

package com.google.cloud.pso.pipeline.model;

import javax.xml.bind.annotation.XmlAttribute;
import java.util.Objects;

public class Address {

    @XmlAttribute(name = "address")
    public String address = null;

    @XmlAttribute(name = "type")
    public String type = null;


    public Address(String address, String type) {
        this.address = address;
        this.type = type;
    }

    public Address(){

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Address address1 = (Address) o;
        return Objects.equals(address, address1.address) &&
                Objects.equals(type, address1.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, type);
    }

    @Override
    public String toString() {
        return "Address{" +
                "address='" + address + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
