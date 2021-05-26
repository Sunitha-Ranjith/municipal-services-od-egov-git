package org.egov.pgr.model;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
@Builder
public class Email {
	private Set<String> emailTo;
	private String subject;
	private String body;

	@JsonProperty("isHTML")
	private boolean isHTML;
}
