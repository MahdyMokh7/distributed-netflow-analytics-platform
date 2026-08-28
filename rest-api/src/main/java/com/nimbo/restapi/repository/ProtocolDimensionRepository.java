package com.nimbo.restapi.repository;

import com.nimbo.restapi.model.ProtocolDimension;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ProtocolDimensionRepository extends JpaRepository<ProtocolDimension, Integer> {

    Optional<ProtocolDimension> findByName(String name);

    Optional<ProtocolDimension> findById(Integer id);
}