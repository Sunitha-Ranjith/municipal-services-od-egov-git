package org.egov.wscalculation.validator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.egov.common.contract.request.RequestInfo;
import org.egov.common.contract.request.User;
import org.egov.mdms.model.MdmsCriteriaReq;
import org.egov.tracer.model.CustomException;
import org.egov.wscalculation.constants.MRConstants;
import org.egov.wscalculation.web.models.MeterConnectionRequest;
import org.egov.wscalculation.web.models.MeterReading;
import org.egov.wscalculation.repository.ServiceRequestRepository;
import org.egov.wscalculation.util.CalculatorUtil;
import org.egov.wscalculation.util.MeterReadingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.jayway.jsonpath.JsonPath;

import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONArray;

@Slf4j
@Component
public class MDMSValidator {

	@Autowired
	private MeterReadingUtil meterReadingUtil;
	
	@Autowired
	private CalculatorUtil calculationUtil;

	@Autowired
	private ServiceRequestRepository serviceRequestRepository;

	@Value("${egov.mdms.host}")
	private String mdmsHost;

	@Value("${egov.mdms.search.endpoint}")
	private String mdmsEndpoint;

	public void validateMasterData(MeterConnectionRequest request) {
		String jsonPath = MRConstants.JSONPATH_ROOT;
		String tenantId = request.getRequestInfo().getUserInfo().getTenantId();

		String[] masterNames = { MRConstants.MDMS_MS_BILLING_PERIOD };
		List<String> names = new ArrayList<>(Arrays.asList(masterNames));
		Map<String, List<String>> codes = getAttributeValues(tenantId, MRConstants.MDMS_WC_MOD_NAME, names,
				"$.*.billingCycle", jsonPath, request.getRequestInfo());
		validateMDMSData(masterNames, codes);
		validateCodes(request.getMeterReading(), codes);
		
	}
	
	public Object validateMasterDataWithoutFilter(String tenantId) {
		RequestInfo requestInfo = new RequestInfo();
		User user = new User();
		user.setTenantId(tenantId);
		requestInfo.setUserInfo(user);
		String[] masterNames = { MRConstants.MDMS_MS_BILLING_PERIOD };
		List<String> names = new ArrayList<>(Arrays.asList(masterNames));
		return getAttributeValuesWithoutFilter(tenantId, MRConstants.MDMS_WC_MOD_NAME, names, "$.*.connectionType",
				MRConstants.JSONPATH_ROOT, requestInfo);
	}
	


	public Map<String, List<String>> getAttributeValues(String tenantId, String moduleName, List<String> names,
			String filter, String jsonPath, RequestInfo requestInfo) {
		StringBuilder uri = new StringBuilder(mdmsHost).append(mdmsEndpoint);
		MdmsCriteriaReq criteriaReq = meterReadingUtil.prepareMdMsRequest(tenantId, moduleName, names, filter,
				requestInfo);
		try {
			Object result = serviceRequestRepository.fetchResult(uri, criteriaReq);
			return JsonPath.read(result, jsonPath);
		} catch (Exception e) {
			log.error("Error while fetching MDMS data", e);
			throw new CustomException(MRConstants.INVALID_BILLING_PERIOD, MRConstants.INVALID_BILLING_PERIOD_MSG);
		}
	}

	private void validateMDMSData(String[] masterNames, Map<String, List<String>> codes) {
		Map<String, String> errorMap = new HashMap<>();
		for (String masterName : masterNames) {
			if (CollectionUtils.isEmpty(codes.get(masterName))) {
				errorMap.put("MDMS DATA ERROR ", "Unable to fetch " + masterName + " codes from MDMS");
			}
		}
		if (!errorMap.isEmpty())
			throw new CustomException(errorMap);
	}

	private  void validateCodes(MeterReading meterReading, Map<String, List<String>> codes) {
		Map<String, String> errorMap = new HashMap<>();
		if (!codes.get(MRConstants.MDMS_MS_BILLING_PERIOD).contains(meterReading.getBillingPeriod())
				&& meterReading.getBillingPeriod() != null) {
			errorMap.put("INVALID_BILLING_PERIOD",
					"The Billing period" + meterReading.getBillingPeriod() + " does not exist");
		}
		if (!errorMap.isEmpty())
			throw new CustomException(errorMap);
	}
	
	private Object getAttributeValuesWithoutFilter(String tenantId, String moduleName, List<String> names,
			String filter, String jsonpath, RequestInfo requestInfo) {
		StringBuilder uri = new StringBuilder(mdmsHost).append(mdmsEndpoint);
		MdmsCriteriaReq criteriaReq = meterReadingUtil.prepareMdMsRequest(tenantId, moduleName, names, filter,
				requestInfo);
		try {
			Object result = serviceRequestRepository.fetchResult(uri, criteriaReq);
			return JsonPath.read(result, jsonpath);
		} catch (Exception e) {
			log.error("Error while fetching MDMS data", e);
			throw new CustomException(MRConstants.INVALID_BILLING_PERIOD, MRConstants.INVALID_BILLING_PERIOD_MSG);
		}
	}

	public void validateAnnualAdvaceMasterData(Object annualAdvanceMaster) {
		Map<String, String> errorMap = new HashMap<>();
		String finYear = calculationUtil.getFinancialYear();
		LinkedHashMap<String, Object> applicableMasterData = getApplicableMasterData(annualAdvanceMaster, finYear);
		
		if(applicableMasterData == null) {
			errorMap.put("INVALID_MASTER_DATA", "No configuration found for this financial year");
		} else {
			Integer annualRebate = (Integer) applicableMasterData.get("annualRebate");
			Boolean isActive = (Boolean) applicableMasterData.get("isActive");
			Long startingDay = (Long) applicableMasterData.get("startingDay");
			Long endingDay = (Long) applicableMasterData.get("endingDay");
			
			if(isActive == null || isActive==Boolean.FALSE) {
				errorMap.put("INVALID_MASTER_DATA", "Annual Advance payment window is not activate");
			}
			
			Long currentTime = System.currentTimeMillis();
			if(startingDay > currentTime || currentTime > endingDay) {
				errorMap.put("INVALID_MASTER_DATA", "Annual Advance payment window is not activate");
			}
			
			if(annualRebate == null) {
				errorMap.put("INVALID_MASTER_DATA", "Annual Advance configuration is wrong");
			}
		}
		
		if (!errorMap.isEmpty())
			throw new CustomException(errorMap);
		
	}
	
	private LinkedHashMap<String, Object> getApplicableMasterData(Object annualAdvanceMaster, String finYear) {
		JSONArray annualAdvanceMD = (JSONArray) annualAdvanceMaster;
		LinkedHashMap<String, Object> applicableMasterData = null;
		for (Object annualAdvance : annualAdvanceMD) {
			LinkedHashMap<String, Object> data = (LinkedHashMap<String, Object>) annualAdvance;
			if(finYear.equalsIgnoreCase(data.get("fromFY").toString())) {
				applicableMasterData = data;
				break;
			};
		}
		return applicableMasterData;
	}
	
	
}
