package org.egov.noc.thirdparty.nma.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.egov.noc.config.NOCConfiguration;
import org.egov.noc.repository.ServiceRequestRepository;
import org.egov.noc.thirdparty.model.ThirdPartyNOCPushRequestWrapper;
import org.egov.noc.thirdparty.nma.model.NmaApplicationRequest;
import org.egov.noc.thirdparty.nma.model.NmaArchitectRegistration;
import org.egov.noc.thirdparty.nma.model.NmaUser;
import org.egov.noc.thirdparty.service.ThirdPartyNocPushService;
import org.egov.noc.util.NOCConstants;
import org.egov.noc.web.model.Noc;
import org.egov.noc.web.model.UserSearchResponse;
import org.egov.tracer.model.CustomException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service(NOCConstants.NMA_NOC_TYPE)
public class NmaNocService implements ThirdPartyNocPushService {

	@Autowired
	private NmaArchitectRegistrationService nmaService;

	@Autowired
	private NmaUtility nmaUtility;

	@Autowired
	private NOCConfiguration config;

	@Autowired
	private ServiceRequestRepository serviceRequestRepository;
	
	private static final ArrayList<String> MANDATORY_ADDITIONAL_DETAILS_THIRDPARTY = new ArrayList<String>() {
		{
			add("NameOfTheNearestMonumentOrSite");
			add("DistanceOfTheSiteOfTheConstructionFromProtectedBoundaryOfMonument");
			add("BasementIfAnyProposedWithDetails");
			add("ApproximateDateOfCommencementOfWorks");
			add("ApproximateDurationOfCommencementOfWorks");
			add("PlotSurveyNo");
			add("MaximumHeightOfExistingModernBuildingInCloseVicinityOf");
			// skipping nested fields as of now
		}
	};

	@Override
	public String pushProcess(ThirdPartyNOCPushRequestWrapper infoWrapper) {
		String comments=null;
		// check if noc additionalDetails contains all mandatory fields before pushing to external dept-
		if (!isMandatoryFieldsAndDocumentsPresent(infoWrapper)) {
			comments = "mandatory fields missing in additionalDetails.Cannot push to external dept";
			return comments;
		}
		UserSearchResponse user = infoWrapper.getUserResponse();
		NmaUser nmaUser = NmaUser.builder().architectEmailId(user.getEmailId())
				.architectMobileNo(user.getMobileNumber()).architectName(user.getName())
				.tenantid(infoWrapper.getNoc().getTenantId()).userid(user.getId()).build();
		NmaArchitectRegistration nmaArchitectRegistration= nmaService.validateNmaArchitectRegistration(nmaUser);
		NmaApplicationRequest nmaApplicationRequest = nmaUtility.buildNmaApplicationRequest(infoWrapper,nmaArchitectRegistration);
		String response = null;
		log.debug("Nma application create: " + nmaApplicationRequest);
		response = (String) nmaUtility.fetchResult(getNmaFormRegURL(), nmaApplicationRequest);
//		response="{\"ApplicationStatus\":[{\"Department\":\"MCD\",\"ArchitectEmailId\":\"rezakhan9494@gmail.com\",\"ApplicationUniqueNumebr\":\"ApplicatinId\r\n"
//				+ "like: 1000254785\",\"ProximityStatus\":\"-1\",\"Status\":\"Application Received\",\"ResponseTime\":\"15-07-2021\r\n"
//				+ "13:43:38:PM\",\"Remarks\":\"Submission of Coordinates pending at architect end.\",\"NodeId\":1767,\"NocFileUrl\":\"No\r\n"
//				+ "File\",\"UniqueId\":\"ApplicatinId like: 1000254785\"}]}";
		DocumentContext documentContext = JsonPath.using(Configuration.defaultConfiguration()).parse(response);
		List<String> list = (List<String>) documentContext.read("ApplicationStatus.*.Status");// Message
		String status = null;
		if (list != null && list.size() > 0) {
			status = list.get(0);
		}
		if ("Application Received".equals(status)) {
			return "Application submited to Nma department with token "+nmaArchitectRegistration.getToken();
		} else {
			throw new CustomException(NOCConstants.NMA_ERROR,
					"Error while calling nma system with msg " + documentContext.read("ApplicationStatus.*.Message"));
		}
	}
	
	private boolean isMandatoryFieldsAndDocumentsPresent(ThirdPartyNOCPushRequestWrapper infoWrapper) {
		Noc noc = infoWrapper.getNoc();
		if (Objects.isNull(noc.getAdditionalDetails()) || !(noc.getAdditionalDetails() instanceof Map)) {
			log.info("additionalDetails null or not a map in the noc");
			return false;
		}
		Map<String, Object> additionalDetails = (Map<String, Object>) noc.getAdditionalDetails();
		if (Objects.isNull(additionalDetails.get("thirdPartyNOC"))
				|| !(additionalDetails.get("thirdPartyNOC") instanceof Map)) {
			log.info("additionalDetails does not contain thirdPartyNOC node and thirdPartyNOC not a Map");
			return false;
		}
		Map<String, Object> thirdPartyNocDetails = (Map<String, Object>) additionalDetails.get("thirdPartyNOC");
		for (String key : MANDATORY_ADDITIONAL_DETAILS_THIRDPARTY) {
			if (Objects.isNull(thirdPartyNocDetails.get(key))) {
				log.info("mandatory key:" + key + " not present inside thirdPartyNOC node of additionalDetails");
				return false;
			}
		}
		// all checks done
		return true;
	}

	
	private StringBuilder getNmaFormRegURL() {
		StringBuilder url = new StringBuilder(config.getNmaHost());
		url.append(config.getNmaContextPath());
		url.append(config.getNmaApplicationCreate());
		return url;
	}

}
