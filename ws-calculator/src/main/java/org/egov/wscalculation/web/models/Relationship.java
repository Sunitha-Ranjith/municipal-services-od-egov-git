package org.egov.wscalculation.web.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * The relationship of gaurdian.
 */
public enum Relationship {
  
    FATHER("FATHER"),
		HUSBAND("HUSBAND"),
		MOTHER("MOTHER"),
		BROTHER("BROTHER"),
		SISTER("SISTER"),
		GRANDFATHER("GRANDFATHER"),
		GRANDMOTHER("GRANDMOTHER"),
		FATHERINLAW("FATHERINLAW"),
		MOTHERINLAW("MOTHERINLAW");

  private String value;

  Relationship(String value) {
    this.value = value;
  }

  @Override
  @JsonValue
  public String toString() {
    return String.valueOf(value);
  }

  @JsonCreator
  public static Relationship fromValue(String text) {
    for (Relationship b : Relationship.values()) {
      if (String.valueOf(b.value).equalsIgnoreCase(text)) {
        return b;
      }
    }
    return null;
  }
}

