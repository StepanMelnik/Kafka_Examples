package com.sme.kafka.spring.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Base entity to identify entities.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BaseEntity implements Serializable
{
    private Integer id;

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
    {
        this.id = id;
    }

    public boolean isNew()
    {
        return this.id == null;
    }
}
