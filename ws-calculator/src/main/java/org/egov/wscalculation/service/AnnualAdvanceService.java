package org.egov.wscalculation.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.validation.Valid;

import org.egov.common.contract.request.RequestInfo;
import org.egov.wscalculation.constants.WSCalculationConstant;
import org.egov.wscalculation.repository.WSCalculationDao;
import org.egov.wscalculation.util.CalculatorUtil;
import org.egov.wscalculation.validator.MDMSValidator;
import org.egov.wscalculation.validator.WSCalculationValidator;
import org.egov.wscalculation.validator.WSCalculationWorkflowValidator;
import org.egov.wscalculation.web.models.AnnualAdvance.AnnualAdvanceStatus;
import org.egov.wscalculation.web.models.AnnualAdvance;
import org.egov.wscalculation.web.models.AnnualAdvanceRequest;
import org.egov.wscalculation.web.models.AnnualPaymentDetails;
import org.egov.wscalculation.web.models.AuditDetails;
import org.egov.wscalculation.web.models.CalculationCriteria;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.jayway.jsonpath.Criteria;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class AnnualAdvanceService {
	
	@Autowired
	private CalculatorUtil calculatorUtil;
	
	@Autowired
	private WSCalculationWorkflowValidator wsCalulationWorkflowValidator;
	
	@Autowired
	private WSCalculationValidator wsCalculationValidator;
	
	@Autowired
	private MDMSValidator mdmsValidator;
	
	@Autowired
	private WSCalculationDao wSCalculationDao;
	
	public void enrichRequest(@Valid AnnualAdvanceRequest annualAdvanceRequests) {
		annualAdvanceRequests.getAnnualAdvance().setId(UUID.randomUUID().toString());
		annualAdvanceRequests.getAnnualAdvance().setFinancialYear(calculatorUtil.getFinancialYear());
		annualAdvanceRequests.getAnnualAdvance().setStatus(AnnualAdvanceStatus.ACTIVE);
		
		if(StringUtils.isEmpty(annualAdvanceRequests.getAnnualAdvance().getChannel())) {
			annualAdvanceRequests.getAnnualAdvance().setChannel(WSCalculationConstant.CHANNEL_SUJOG);
		}
		
		long currentTime = System.currentTimeMillis();
		AuditDetails auditDetails = AuditDetails.builder()
				.createdBy(annualAdvanceRequests.getRequestInfo().getUserInfo().getUuid())
				.createdTime(currentTime)
				.lastModifiedBy(annualAdvanceRequests.getRequestInfo().getUserInfo().getUuid())
				.lastModifiedTime(currentTime).build();
		annualAdvanceRequests.getAnnualAdvance().setAuditDetails(auditDetails);
	}
	
	public void applicationValidation(RequestInfo requestInfo, List<CalculationCriteria> calculationCriteria, Map<String, Object> masterMap) {
		CalculationCriteria criteria = calculationCriteria.get(0);
		mdmsValidator.validateAnnualAdvaceMasterData(masterMap.get(WSCalculationConstant.WC_ANNUAL_ADVANCE_MASTER));
		wsCalulationWorkflowValidator.waterApplicationValidationForAnnualAdvance(requestInfo, criteria.getTenantId(), criteria.getConnectionNo());
		wsCalculationValidator.validateAnnualAdvance(requestInfo, criteria.getTenantId(), criteria.getConnectionNo());
	}

	public void enrichAnnualAdvanceDetails(@Valid AnnualAdvanceRequest annualAdvanceRequests,
			AnnualPaymentDetails annualPaymentDetails) {
		HashMap<String, Object> additionalDetail = new HashMap<>();
		additionalDetail.put(WSCalculationConstant.ADVANCE_WATER_CHARGE, annualPaymentDetails.getTotalWaterCharge());
		additionalDetail.put(WSCalculationConstant.ADVANCE_SEWERAGE_CHARGE, annualPaymentDetails.getTotalSewerageCharge());
		additionalDetail.put(WSCalculationConstant.ADVANCE_REBATE, annualPaymentDetails.getTotalRebate());
		
		annualAdvanceRequests.getAnnualAdvance().setAdditionalDetails(additionalDetail);
	}

	public List<AnnualAdvance> findAnnualPayment(String tenantId, String connectionNo, String assessYear) {
		if(StringUtils.isEmpty(assessYear)) {
			assessYear = calculatorUtil.getFinancialYear();
		}
		
		List<AnnualAdvance> annualAdvances = wSCalculationDao.getAnnualAdvance(tenantId, connectionNo, assessYear);
		return annualAdvances;
	}

}
