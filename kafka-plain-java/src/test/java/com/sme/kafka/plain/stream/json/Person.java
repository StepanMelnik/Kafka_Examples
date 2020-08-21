package com.sme.kafka.plain.stream.json;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

/**
 * Represents Person POJO.
 */
@JsonRootName("person")
public class Person implements Serializable
{
    private static final long serialVersionUID = 1L;

    private String personalID;
    private String name;
    private String country;
    private String occupation;

    public Person()
    {
    }

    public @JsonCreator Person(@JsonProperty("personalID") String personalID, @JsonProperty("name") String name,
            @JsonProperty("country") String country, @JsonProperty("occupation") String occupation)
    {
        this.name = name;
        this.personalID = personalID;
        this.country = country;
        this.occupation = occupation;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getPersonalID()
    {
        return personalID;
    }

    public void setPersonalID(String personalID)
    {
        this.personalID = personalID;
    }

    public String getCountry()
    {
        return country;
    }

    public void setCountry(String country)
    {
        this.country = country;
    }

    public String getOccupation()
    {
        return occupation;
    }

    public void setOccupation(String occupation)
    {
        this.occupation = occupation;
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj)
    {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
