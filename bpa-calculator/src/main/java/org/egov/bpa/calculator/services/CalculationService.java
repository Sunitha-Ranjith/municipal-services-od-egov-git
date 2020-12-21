package org.egov.bpa.calculator.services;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.egov.bpa.calculator.config.BPACalculatorConfig;
import org.egov.bpa.calculator.kafka.broker.BPACalculatorProducer;
import org.egov.bpa.calculator.utils.BPACalculatorConstants;
import org.egov.bpa.calculator.utils.CalculationUtils;
import org.egov.bpa.calculator.web.models.BillingSlabSearchCriteria;
import org.egov.bpa.calculator.web.models.Calculation;
import org.egov.bpa.calculator.web.models.CalculationReq;
import org.egov.bpa.calculator.web.models.CalculationRes;
import org.egov.bpa.calculator.web.models.CalulationCriteria;
import org.egov.bpa.calculator.web.models.bpa.BPA;
import org.egov.bpa.calculator.web.models.bpa.EstimatesAndSlabs;
import org.egov.bpa.calculator.web.models.demand.Category;
import org.egov.bpa.calculator.web.models.demand.TaxHeadEstimate;
import org.egov.common.contract.request.RequestInfo;
import org.egov.tracer.model.CustomException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONArray;

@Service
@Slf4j
public class CalculationService {
	

	@Autowired
	private MDMSService mdmsService;

	@Autowired
	private DemandService demandService;

	@Autowired
	private EDCRService edcrService;

	@Autowired
	private BPACalculatorConfig config;

	@Autowired
	private CalculationUtils utils;

	@Autowired
	private BPACalculatorProducer producer;

	@Autowired
	private BPAService bpaService;

	private static final BigDecimal ZERO_TWO_FIVE = BigDecimal.valueOf(0.25);
	private static final BigDecimal TEN = BigDecimal.valueOf(10);
	private static final BigDecimal FIFTEEN = BigDecimal.valueOf(15);
	private static final BigDecimal SEVENTEEN_FIVE = BigDecimal.valueOf(17.5);
	private static final BigDecimal TWENTY = BigDecimal.valueOf(20);
	private static final BigDecimal TWENTY_FIVE = BigDecimal.valueOf(25);
	private static final BigDecimal THIRTY = BigDecimal.valueOf(30);
	private static final BigDecimal FIFTY = BigDecimal.valueOf(50);
	private static final BigDecimal HUNDRED = BigDecimal.valueOf(100);
	private static final BigDecimal TWO_HUNDRED = BigDecimal.valueOf(200);
	private static final BigDecimal TWO_HUNDRED_FIFTY = BigDecimal.valueOf(250);
	private static final BigDecimal THREE_HUNDRED = BigDecimal.valueOf(300);
	private static final BigDecimal FIVE_HUNDRED = BigDecimal.valueOf(500);
	private static final BigDecimal FIFTEEN_HUNDRED = BigDecimal.valueOf(1500);
	private static final BigDecimal SQMT_SQFT_MULTIPLIER = BigDecimal.valueOf(10.764);
	private static final BigDecimal ACRE_SQMT_MULTIPLIER = BigDecimal.valueOf(4046.85);

	/**
	 * Calculates tax estimates and creates demand
	 * 
	 * @param calculationReq The calculationCriteria request
	 * @return List of calculations for all applicationNumbers or tradeLicenses in
	 *         calculationReq
	 */
	public List<Calculation> calculate(CalculationReq calculationReq) {
		String tenantId = calculationReq.getCalulationCriteria().get(0).getTenantId();
		Object mdmsData = mdmsService.mDMSCall(calculationReq, tenantId);
		// List<Calculation> calculations =
		// getCalculation(calculationReq.getRequestInfo(),calculationReq.getCalulationCriteria(),
		// mdmsData);
		List<Calculation> calculations = getCalculationV2(calculationReq.getRequestInfo(),
				calculationReq.getCalulationCriteria());
		demandService.generateDemand(calculationReq.getRequestInfo(), calculations, mdmsData);
		CalculationRes calculationRes = CalculationRes.builder().calculations(calculations).build();
		producer.push(config.getSaveTopic(), calculationRes);
		return calculations;
	}

	private List<Calculation> getCalculationV2(RequestInfo requestInfo, List<CalulationCriteria> calulationCriteria) {
		List<Calculation> calculations = new LinkedList<>();
		if (!CollectionUtils.isEmpty(calulationCriteria)) {
			for (CalulationCriteria criteria : calulationCriteria) {
				BPA bpa;
				if (criteria.getBpa() == null && criteria.getApplicationNo() != null) {
					bpa = bpaService.getBuildingPlan(requestInfo, criteria.getTenantId(), criteria.getApplicationNo(),
							null);
					criteria.setBpa(bpa);
				}

				EstimatesAndSlabs estimatesAndSlabs = getTaxHeadEstimatesV2(criteria, requestInfo);
				List<TaxHeadEstimate> taxHeadEstimates = estimatesAndSlabs.getEstimates();

				Calculation calculation = new Calculation();
				calculation.setBpa(criteria.getBpa());
				calculation.setTenantId(criteria.getTenantId());
				calculation.setTaxHeadEstimates(taxHeadEstimates);
				calculation.setFeeType(criteria.getFeeType());
				calculations.add(calculation);

			}

		}
		return calculations;
	}

	private EstimatesAndSlabs getTaxHeadEstimatesV2(CalulationCriteria criteria, RequestInfo requestInfo) {
		List<TaxHeadEstimate> estimates = new LinkedList<>();
		EstimatesAndSlabs estimatesAndSlabs;
		estimatesAndSlabs = getBaseTaxV2(criteria, requestInfo);
		estimates.addAll(estimatesAndSlabs.getEstimates());
		estimatesAndSlabs.setEstimates(estimates);

		return estimatesAndSlabs;
	}

	/***
	 * Calculates tax estimates
	 * 
	 * @param requestInfo The requestInfo of the calculation request
	 * @param criterias   list of CalculationCriteria containing the tradeLicense or
	 *                    applicationNumber
	 * @return List of calculations for all applicationNumbers or tradeLicenses in
	 *         criterias
	 */
	public List<Calculation> getCalculation(RequestInfo requestInfo, List<CalulationCriteria> criterias,
			Object mdmsData) {
		List<Calculation> calculations = new LinkedList<>();
		for (CalulationCriteria criteria : criterias) {
			BPA bpa;
			if (criteria.getBpa() == null && criteria.getApplicationNo() != null) {
				bpa = bpaService.getBuildingPlan(requestInfo, criteria.getTenantId(), criteria.getApplicationNo(),
						null);
				criteria.setBpa(bpa);
			}

			EstimatesAndSlabs estimatesAndSlabs = getTaxHeadEstimates(criteria, requestInfo, mdmsData);
			List<TaxHeadEstimate> taxHeadEstimates = estimatesAndSlabs.getEstimates();

			Calculation calculation = new Calculation();
			calculation.setBpa(criteria.getBpa());
			calculation.setTenantId(criteria.getTenantId());
			calculation.setTaxHeadEstimates(taxHeadEstimates);
			calculation.setFeeType(criteria.getFeeType());
			calculations.add(calculation);

		}
		return calculations;
	}

	/**
	 * Creates TacHeadEstimates
	 * 
	 * @param calulationCriteria CalculationCriteria containing the tradeLicense or
	 *                           applicationNumber
	 * @param requestInfo        The requestInfo of the calculation request
	 * @return TaxHeadEstimates and the billingSlabs used to calculate it
	 */
	private EstimatesAndSlabs getTaxHeadEstimates(CalulationCriteria calulationCriteria, RequestInfo requestInfo,
			Object mdmsData) {
		List<TaxHeadEstimate> estimates = new LinkedList<>();
		EstimatesAndSlabs estimatesAndSlabs;
		if (calulationCriteria.getFeeType().equalsIgnoreCase(BPACalculatorConstants.LOW_RISK_PERMIT_FEE_TYPE)) {

//			 stopping Application fee for lowrisk applicaiton according to BBI-391
			calulationCriteria.setFeeType(BPACalculatorConstants.MDMS_CALCULATIONTYPE_LOW_APL_FEETYPE);
			estimatesAndSlabs = getBaseTax(calulationCriteria, requestInfo, mdmsData);

			estimates.addAll(estimatesAndSlabs.getEstimates());

			calulationCriteria.setFeeType(BPACalculatorConstants.MDMS_CALCULATIONTYPE_LOW_SANC_FEETYPE);
			estimatesAndSlabs = getBaseTax(calulationCriteria, requestInfo, mdmsData);

			estimates.addAll(estimatesAndSlabs.getEstimates());

			calulationCriteria.setFeeType(BPACalculatorConstants.LOW_RISK_PERMIT_FEE_TYPE);

		} else {
			estimatesAndSlabs = getBaseTax(calulationCriteria, requestInfo, mdmsData);
			estimates.addAll(estimatesAndSlabs.getEstimates());
		}

		estimatesAndSlabs.setEstimates(estimates);

		return estimatesAndSlabs;
	}

	/**
	 * Calculates base tax and cretaes its taxHeadEstimate
	 * 
	 * @param calulationCriteria CalculationCriteria containing the tradeLicense or
	 *                           applicationNumber
	 * @param requestInfo        The requestInfo of the calculation request
	 * @return BaseTax taxHeadEstimate and billingSlabs used to calculate it
	 */
	@SuppressWarnings({ "rawtypes" })
	private EstimatesAndSlabs getBaseTax(CalulationCriteria calulationCriteria, RequestInfo requestInfo,
			Object mdmsData) {
		BPA bpa = calulationCriteria.getBpa();
		EstimatesAndSlabs estimatesAndSlabs = new EstimatesAndSlabs();
		BillingSlabSearchCriteria searchCriteria = new BillingSlabSearchCriteria();
		searchCriteria.setTenantId(bpa.getTenantId());

		Map calculationTypeMap = mdmsService.getCalculationType(requestInfo, bpa, mdmsData,
				calulationCriteria.getFeeType());
		int calculatedAmout = 0;
		ArrayList<TaxHeadEstimate> estimates = new ArrayList<TaxHeadEstimate>();
		if (calculationTypeMap.containsKey("calsiLogic")) {
			LinkedHashMap ocEdcr = edcrService.getEDCRDetails(requestInfo, bpa);
			String jsonString = new JSONObject(ocEdcr).toString();
			DocumentContext context = JsonPath.using(Configuration.defaultConfiguration()).parse(jsonString);
			JSONArray permitNumber = context.read("edcrDetail.*.permitNumber");
			String jsonData = new JSONObject(calculationTypeMap).toString();
			DocumentContext calcContext = JsonPath.using(Configuration.defaultConfiguration()).parse(jsonData);
			JSONArray parameterPaths = calcContext.read("calsiLogic.*.paramPath");
			JSONArray tLimit = calcContext.read("calsiLogic.*.tolerancelimit");
			System.out.println("tolerance limit in: " + tLimit.get(0));
			DocumentContext edcrContext = null;
			if (!CollectionUtils.isEmpty(permitNumber)) {
				BPA permitBpa = bpaService.getBuildingPlan(requestInfo, bpa.getTenantId(), null,
						permitNumber.get(0).toString());
				if (permitBpa.getEdcrNumber() != null) {
					LinkedHashMap edcr = edcrService.getEDCRDetails(requestInfo, permitBpa);
					String edcrData = new JSONObject(edcr).toString();
					edcrContext = JsonPath.using(Configuration.defaultConfiguration()).parse(edcrData);
				}
			}

			for (int i = 0; i < parameterPaths.size(); i++) {
				Double ocTotalBuitUpArea = context.read(parameterPaths.get(i).toString());
				Double bpaTotalBuitUpArea = edcrContext.read(parameterPaths.get(i).toString());
				Double diffInBuildArea = ocTotalBuitUpArea - bpaTotalBuitUpArea;
				System.out.println("difference in area: " + diffInBuildArea);
				Double limit = Double.valueOf(tLimit.get(i).toString());
				if (diffInBuildArea > limit) {
					JSONArray data = calcContext.read("calsiLogic.*.deviation");
					System.out.println(data.get(0));
					JSONArray data1 = (JSONArray) data.get(0);
					for (int j = 0; j < data1.size(); j++) {
						LinkedHashMap diff = (LinkedHashMap) data1.get(j);
						Integer from = (Integer) diff.get("from");
						Integer to = (Integer) diff.get("to");
						Integer uom = (Integer) diff.get("uom");
						Integer mf = (Integer) diff.get("MF");
						if (diffInBuildArea >= from && diffInBuildArea <= to) {
							calculatedAmout = (int) (diffInBuildArea * mf * uom);
							break;
						}
					}
				} else {
					calculatedAmout = 0;
				}
				TaxHeadEstimate estimate = new TaxHeadEstimate();
				BigDecimal totalTax = BigDecimal.valueOf(calculatedAmout);
				if (totalTax.compareTo(BigDecimal.ZERO) == -1)
					throw new CustomException(BPACalculatorConstants.INVALID_AMOUNT, "Tax amount is negative");

				estimate.setEstimateAmount(totalTax);
				estimate.setCategory(Category.FEE);

				String taxHeadCode = utils.getTaxHeadCode(bpa.getBusinessService(), calulationCriteria.getFeeType());
				estimate.setTaxHeadCode(taxHeadCode);
				estimates.add(estimate);
			}
		} else {
			TaxHeadEstimate estimate = new TaxHeadEstimate();
			calculatedAmout = Integer
					.parseInt(calculationTypeMap.get(BPACalculatorConstants.MDMS_CALCULATIONTYPE_AMOUNT).toString());

			BigDecimal totalTax = BigDecimal.valueOf(calculatedAmout);
			if (totalTax.compareTo(BigDecimal.ZERO) == -1)
				throw new CustomException(BPACalculatorConstants.INVALID_AMOUNT, "Tax amount is negative");

			estimate.setEstimateAmount(totalTax);
			estimate.setCategory(Category.FEE);

			String taxHeadCode = utils.getTaxHeadCode(bpa.getBusinessService(), calulationCriteria.getFeeType());
			estimate.setTaxHeadCode(taxHeadCode);
			estimates.add(estimate);
		}
		estimatesAndSlabs.setEstimates(estimates);
		return estimatesAndSlabs;
	}

	private EstimatesAndSlabs getBaseTaxV2(CalulationCriteria criteria, RequestInfo requestInfo) {
		BPA bpa = criteria.getBpa();
		String feeType = criteria.getFeeType();

		EstimatesAndSlabs estimatesAndSlabs = new EstimatesAndSlabs();
		BillingSlabSearchCriteria searchCriteria = new BillingSlabSearchCriteria();
		searchCriteria.setTenantId(bpa.getTenantId());

		ArrayList<TaxHeadEstimate> estimates = new ArrayList<TaxHeadEstimate>();

		if (StringUtils.hasText(feeType)
				&& feeType.equalsIgnoreCase(BPACalculatorConstants.MDMS_CALCULATIONTYPE_APL_FEETYPE)) {
			calculateTotalFee(requestInfo, criteria, estimates,
					BPACalculatorConstants.MDMS_CALCULATIONTYPE_APL_FEETYPE);

		}
		if (StringUtils.hasText(feeType)
				&& feeType.equalsIgnoreCase(BPACalculatorConstants.MDMS_CALCULATIONTYPE_SANC_FEETYPE)) {

			calculateTotalFee(requestInfo, criteria, estimates,
					BPACalculatorConstants.MDMS_CALCULATIONTYPE_SANC_FEETYPE);
		}

		estimatesAndSlabs.setEstimates(estimates);
		return estimatesAndSlabs;

	}

	/**
	 * @param requestInfo
	 * @param criteria
	 * @param bpa
	 * @param estimates
	 * @param mdmsCalculationtypeAplFeetype
	 */
	private void calculateTotalFee(RequestInfo requestInfo, CalulationCriteria criteria,
			ArrayList<TaxHeadEstimate> estimates, String feeType) {

		Map<String, Object> paramMap = prepareMaramMap(requestInfo, criteria, feeType);

		BigDecimal calculatedTotalAmout = calculateTotalFeeAmount(paramMap);

		TaxHeadEstimate estimate = new TaxHeadEstimate();

		if (calculatedTotalAmout.compareTo(BigDecimal.ZERO) == -1) {
			throw new CustomException(BPACalculatorConstants.INVALID_AMOUNT, "Tax amount is negative");
		}

		estimate.setEstimateAmount(calculatedTotalAmout);
		estimate.setCategory(Category.FEE);

		String taxHeadCode = utils.getTaxHeadCode(criteria.getBpa().getBusinessService(), criteria.getFeeType());
		estimate.setTaxHeadCode(taxHeadCode);
		estimates.add(estimate);
	}

	/**
	 * @param requestInfo
	 * @param criteria
	 * @param feeType
	 * @return
	 */
	private Map<String, Object> prepareMaramMap(RequestInfo requestInfo, CalulationCriteria criteria, String feeType) {
		String applicationType = criteria.getApplicationType();
		String serviceType = criteria.getServiceType();
		String riskType = criteria.getBpa().getRiskType();

		@SuppressWarnings("unchecked")
		LinkedHashMap<String, Object> edcr = edcrService.getEDCRDetails(requestInfo, criteria.getBpa());
		String jsonString = new JSONObject(edcr).toString();
		DocumentContext context = JsonPath.using(Configuration.defaultConfiguration()).parse(jsonString);

		Map<String, Object> paramMap = new HashMap<>();

		JSONArray occupancyTypeJSONArray = context.read(BPACalculatorConstants.OCCUPANCY_TYPE_PATH);
		if (!CollectionUtils.isEmpty(occupancyTypeJSONArray)) {
			String occupancyType = occupancyTypeJSONArray.get(0).toString();
			paramMap.put(BPACalculatorConstants.OCCUPANCY_TYPE, occupancyType);
		}

		JSONArray subOccupancyTypeJSONArray = context.read(BPACalculatorConstants.SUB_OCCUPANCY_TYPE_PATH);
		if (!CollectionUtils.isEmpty(subOccupancyTypeJSONArray)) {
			String subOccupancyType = subOccupancyTypeJSONArray.get(0).toString();
			paramMap.put(BPACalculatorConstants.SUB_OCCUPANCY_TYPE, subOccupancyType);

		}

		JSONArray plotAreas = context.read(BPACalculatorConstants.PLOT_AREA_PATH);
		if (!CollectionUtils.isEmpty(plotAreas)) {
			Double plotArea = (Double) plotAreas.get(0);
			paramMap.put(BPACalculatorConstants.PLOT_AREA, plotArea);
		}

		JSONArray totalBuitUpAreas = context.read(BPACalculatorConstants.BUILTUP_AREA_PATH);
		if (!CollectionUtils.isEmpty(totalBuitUpAreas)) {
			Double totalBuitUpArea = (Double) totalBuitUpAreas.get(0);
			paramMap.put(BPACalculatorConstants.BUILTUP_AREA, totalBuitUpArea);
		}

		JSONArray totalEWSAreas = context.read(BPACalculatorConstants.EWS_AREA_PATH);
		if (!CollectionUtils.isEmpty(totalEWSAreas)) {
			Double totalEWSArea = (Double) totalEWSAreas.get(0);
			paramMap.put(BPACalculatorConstants.EWS_AREA, totalEWSArea);
		}

		JSONArray totalbenchmarkValuePerAcre = context.read(BPACalculatorConstants.BENCHMARK_VALUE_PATH);
		if (!CollectionUtils.isEmpty(totalbenchmarkValuePerAcre)) {
			Integer benchmarkValuePerAcre = (Integer) totalbenchmarkValuePerAcre.get(0);
			paramMap.put(BPACalculatorConstants.BMV_ACRE, benchmarkValuePerAcre);
		}

		JSONArray totalbaseFar = context.read(BPACalculatorConstants.BASE_FAR_PATH);
		if (!CollectionUtils.isEmpty(totalbaseFar)) {
			Integer baseFar = (Integer) totalbaseFar.get(0);
			paramMap.put(BPACalculatorConstants.BASE_FAR, baseFar);
		}

		JSONArray totalpermissibleFar = context.read(BPACalculatorConstants.PERMISSIBLE_FAR_PATH);
		if (!CollectionUtils.isEmpty(totalpermissibleFar)) {
			Integer permissibleFar = (Integer) totalpermissibleFar.get(0);
			paramMap.put(BPACalculatorConstants.PERMISSIBLE_FAR, permissibleFar);
		}

		JSONArray totalNoOfDwellingUnitsArray = context.read(BPACalculatorConstants.DWELLING_UNITS_PATH);
		if (!CollectionUtils.isEmpty(totalNoOfDwellingUnitsArray)) {
			Integer totalNoOfDwellingUnits = (Integer) totalNoOfDwellingUnitsArray.get(0);
			paramMap.put(BPACalculatorConstants.TOTAL_NO_OF_DWELLING_UNITS, totalNoOfDwellingUnits);
		}

		JSONArray isShelterFeeRequiredArray = context.read(BPACalculatorConstants.SHELTER_FEE_PATH);
		if (!CollectionUtils.isEmpty(isShelterFeeRequiredArray)) {
			boolean isShelterFeeRequired = (boolean) isShelterFeeRequiredArray.get(0);
			paramMap.put(BPACalculatorConstants.SHELTER_FEE, isShelterFeeRequired);
		}

		paramMap.put(BPACalculatorConstants.APPLICATION_TYPE, applicationType);
		paramMap.put(BPACalculatorConstants.SERVICE_TYPE, serviceType);
		paramMap.put(BPACalculatorConstants.RISK_TYPE, riskType);
		paramMap.put(BPACalculatorConstants.FEE_TYPE, feeType);
		return paramMap;
	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param riskType
	 * @param occupancyType
	 * @param subOccupancyType
	 * @param plotArea
	 * @param totalBuitUpArea
	 * @param feeType
	 * @return
	 */
	private BigDecimal calculateTotalFeeAmount(Map<String, Object> paramMap) {

		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String feeType = (String) paramMap.get(BPACalculatorConstants.FEE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);

		BigDecimal calculatedTotalAmout = BigDecimal.ZERO;

		if (StringUtils.hasText(applicationType) && (StringUtils.hasText(serviceType))
				&& StringUtils.hasText(occupancyType) && (StringUtils.hasText(feeType))) {
			if (feeType.equalsIgnoreCase(BPACalculatorConstants.MDMS_CALCULATIONTYPE_APL_FEETYPE)) {
				calculatedTotalAmout = calculateTotalScrutinyFee(paramMap);

			} else if (feeType.equalsIgnoreCase(BPACalculatorConstants.MDMS_CALCULATIONTYPE_SANC_FEETYPE)) {
				calculatedTotalAmout = calculateTotalPermitFee(paramMap);
			}

		}

		return calculatedTotalAmout;
	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param riskType
	 * @param occupancyType
	 * @param subOccupancyType
	 * @param plotArea
	 * @param totalBuitUpArea
	 * @return
	 */
	private BigDecimal calculateTotalPermitFee(Map<String, Object> paramMap) {

		BigDecimal calculatedTotalPermitFee = BigDecimal.ZERO;
		BigDecimal sanctionFee = calculateSanctionFee(paramMap);
		BigDecimal constructionWorkerWelfareCess = calculateConstructionWorkerWelfareCess(paramMap);
		BigDecimal shelterFee = calculateShelterFee(paramMap);
		BigDecimal temporaryRetentionFee = calculateTemporaryRetentionFee(paramMap);
		BigDecimal securityDeposit = calculateSecurityDeposit(paramMap);
		BigDecimal purchasableFAR = calculatePurchasableFAR(paramMap);

		calculatedTotalPermitFee = (calculatedTotalPermitFee.add(sanctionFee).add(constructionWorkerWelfareCess)
				.add(shelterFee).add(temporaryRetentionFee).add(securityDeposit).add(purchasableFAR)).setScale(2,
						BigDecimal.ROUND_UP);

		return calculatedTotalPermitFee;
	}

	/**
	 * @param paramMap
	 * @return
	 */
	private BigDecimal calculatePurchasableFAR(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		Double benchmarkValuePerAcre = (Double) paramMap.get(BPACalculatorConstants.BMV_ACRE);
		Double providedFar = (Double) paramMap.get(BPACalculatorConstants.BASE_FAR);
		Double permissableFar = (Double) paramMap.get(BPACalculatorConstants.PERMISSIBLE_FAR);

		BigDecimal purchasableFARFee = BigDecimal.ZERO;
		if ((StringUtils.hasText(applicationType)
				&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
				&& (StringUtils.hasText(serviceType)
						&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {

			BigDecimal benchmarkValuePerSQM = BigDecimal.valueOf(benchmarkValuePerAcre).divide(ACRE_SQMT_MULTIPLIER, 2,
					BigDecimal.ROUND_HALF_UP);

			BigDecimal purchasableFARRate = (benchmarkValuePerSQM.multiply(ZERO_TWO_FIVE)).setScale(2,
					BigDecimal.ROUND_HALF_UP);

			BigDecimal deltaFAR = (BigDecimal.valueOf(providedFar).subtract(BigDecimal.valueOf(permissableFar)))
					.setScale(2, BigDecimal.ROUND_HALF_UP);

			purchasableFARFee = (purchasableFARRate.multiply(deltaFAR)).setScale(2, BigDecimal.ROUND_HALF_UP);

		}
		return purchasableFARFee;

	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param occupancyType
	 * @param subOccupancyType
	 * @param totalBuitUpArea
	 * @return
	 */
	private BigDecimal calculateSecurityDeposit(Map<String, Object> paramMap) {
		Double totalBuitUpArea = (Double) paramMap.get(BPACalculatorConstants.BUILTUP_AREA);

		BigDecimal securityDeposit = BigDecimal.ZERO;
		if (totalBuitUpArea != null) {

			securityDeposit = calculateSecurityDepositForResidentialOccupancy(paramMap);
			securityDeposit = calculateSecurityDepositForCommercialOccupancy(paramMap);
			securityDeposit = calculateSecurityDepositForPublicSemiPublicInstitutionalOccupancy(paramMap);
			securityDeposit = calculateSecurityDepositForEducationOccupancy(paramMap);

		}
		return securityDeposit;

	}

	private BigDecimal calculateSecurityDepositForEducationOccupancy(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);
		String subOccupancyType = (String) paramMap.get(BPACalculatorConstants.SUB_OCCUPANCY_TYPE);
		Double totalBuitUpArea = (Double) paramMap.get(BPACalculatorConstants.BUILTUP_AREA);

		BigDecimal securityDeposit = BigDecimal.ZERO;
		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.F) && StringUtils.hasText(subOccupancyType))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				if (totalBuitUpArea >= 200) {
					securityDeposit = calculateConstantFee(paramMap, 100);

				}
			}
		}
		return securityDeposit;
	}

	private BigDecimal calculateSecurityDepositForPublicSemiPublicInstitutionalOccupancy(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);
		String subOccupancyType = (String) paramMap.get(BPACalculatorConstants.SUB_OCCUPANCY_TYPE);
		Double totalBuitUpArea = (Double) paramMap.get(BPACalculatorConstants.BUILTUP_AREA);

		BigDecimal securityDeposit = BigDecimal.ZERO;
		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.C) && StringUtils.hasText(subOccupancyType))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				if (totalBuitUpArea >= 200) {
					securityDeposit = calculateConstantFee(paramMap, 100);

				}
			}
		}
		return securityDeposit;
	}

	private BigDecimal calculateSecurityDepositForCommercialOccupancy(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);
		String subOccupancyType = (String) paramMap.get(BPACalculatorConstants.SUB_OCCUPANCY_TYPE);
		Double totalBuitUpArea = (Double) paramMap.get(BPACalculatorConstants.BUILTUP_AREA);

		BigDecimal securityDeposit = BigDecimal.ZERO;
		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.B) && StringUtils.hasText(subOccupancyType))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				if (totalBuitUpArea >= 200) {
					securityDeposit = calculateConstantFee(paramMap, 100);

				}
			}
		}
		return securityDeposit;
	}

	private BigDecimal calculateSecurityDepositForResidentialOccupancy(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);
		String subOccupancyType = (String) paramMap.get(BPACalculatorConstants.SUB_OCCUPANCY_TYPE);

		BigDecimal securityDeposit = BigDecimal.ZERO;

		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.A) && StringUtils.hasText(subOccupancyType))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_AB))
						|| (occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_HP))
						|| occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_WCR)
						|| occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_SA)
						|| occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_E)
						|| occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_LIH)
						|| occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_MIH)
						|| occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_SQ)) {

					securityDeposit = calculateConstantFee(paramMap, 100);

				}
			}

		}
		return securityDeposit;
	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param occupancyType
	 * @param subOccupancyType
	 * @param totalBuitUpArea
	 * @return
	 */
	private BigDecimal calculateTemporaryRetentionFee(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);

		BigDecimal retentionFee = BigDecimal.ZERO;
		if ((StringUtils.hasText(applicationType)
				&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
				&& (StringUtils.hasText(serviceType)
						&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
			retentionFee = BigDecimal.valueOf(2000);
		}
		return retentionFee;

	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param occupancyType
	 * @param subOccupancyType
	 * @param totalBuitUpArea
	 * @return
	 */
	private BigDecimal calculateShelterFee(Map<String, Object> paramMap) {

		Double totalBuitUpArea = (Double) paramMap.get(BPACalculatorConstants.BUILTUP_AREA);
		BigDecimal shelterFee = BigDecimal.ZERO;
		if (totalBuitUpArea != null) {

			shelterFee = calculateShelterFeeForResidentialOccupancy(paramMap);
		}
		return shelterFee;

	}

	private BigDecimal calculateShelterFeeForResidentialOccupancy(Map<String, Object> paramMap) {
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);
		String subOccupancyType = (String) paramMap.get(BPACalculatorConstants.SUB_OCCUPANCY_TYPE);

		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		Double totalEWSArea = (Double) paramMap.get(BPACalculatorConstants.EWS_AREA);
		boolean isShelterFeeRequired = (boolean) paramMap.get(BPACalculatorConstants.SHELTER_FEE);
		int totalNoOfDwellingUnits = (int) paramMap.get(BPACalculatorConstants.TOTAL_NO_OF_DWELLING_UNITS);

		BigDecimal shelterFee = BigDecimal.ZERO;
		if (isShelterFeeRequired && totalNoOfDwellingUnits > 8) {
			if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.A) && StringUtils.hasText(subOccupancyType))) {
				if ((StringUtils.hasText(applicationType)
						&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
						&& (StringUtils.hasText(serviceType)
								&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
					if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_P))
							|| (occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_S))
							|| (occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_R))
							|| (occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_AB))
							|| (occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_HP))
							|| occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_WCR)
							|| occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_SA)
							|| occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_MIH)) {

						shelterFee = (BigDecimal.valueOf(totalEWSArea).multiply(SQMT_SQFT_MULTIPLIER)
								.multiply(BigDecimal.valueOf(1750)).multiply(ZERO_TWO_FIVE)).setScale(2,
										BigDecimal.ROUND_UP);
					}
				}

			}

		}
		return shelterFee;
	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param occupancyType
	 * @param subOccupancyType
	 * @param totalBuitUpArea
	 * @return
	 */
	private BigDecimal calculateConstructionWorkerWelfareCess(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		Double totalBuitUpArea = (Double) paramMap.get(BPACalculatorConstants.BUILTUP_AREA);

		BigDecimal welfareCess = BigDecimal.ZERO;

		if ((StringUtils.hasText(applicationType)
				&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
				&& (StringUtils.hasText(serviceType)
						&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
			Double costOfConstruction = (1750 * totalBuitUpArea);
			if (costOfConstruction > 1000000) {
				welfareCess = (SEVENTEEN_FIVE.multiply(BigDecimal.valueOf(totalBuitUpArea))).setScale(2,
						BigDecimal.ROUND_UP);
			}

		}
		return welfareCess;

	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param subOccupancyType
	 * @param occupancyType
	 * @param totalBuitUpArea
	 * @return
	 */
	private BigDecimal calculateSanctionFee(Map<String, Object> paramMap) {
		Double totalBuitUpArea = (Double) paramMap.get(BPACalculatorConstants.BUILTUP_AREA);

		BigDecimal sanctionFee = BigDecimal.ZERO;
		if (totalBuitUpArea != null) {

			sanctionFee = calculateSanctionFeeForResidentialOccupancy(paramMap);
			sanctionFee = calculateSanctionFeeForCommercialOccupancy(paramMap);
			sanctionFee = calculateSanctionFeeForPublicSemiPublicInstitutionalOccupancy(paramMap);
			sanctionFee = calculateSanctionFeeForPublicUtilityOccupancy(paramMap);
			sanctionFee = calculateSanctionFeeForIndustrialZoneOccupancy(paramMap);
			sanctionFee = calculateSanctionFeeForEducationOccupancy(paramMap);
			sanctionFee = calculateSanctionFeeForTransportationOccupancy(paramMap);
			sanctionFee = calculateSanctionFeeForAgricultureOccupancy(paramMap);

		}
		return sanctionFee;
	}

	private BigDecimal calculateSanctionFeeForPublicSemiPublicInstitutionalOccupancy(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);
		String subOccupancyType = (String) paramMap.get(BPACalculatorConstants.SUB_OCCUPANCY_TYPE);

		BigDecimal sanctionFee = BigDecimal.ZERO;
		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.C)) && (StringUtils.hasText(subOccupancyType))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				if ((subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_A))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_B))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_C))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_CL))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_MP))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_CH))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_O))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_OAH)
								|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_SC))
								|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_C1H))
								|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_C2H))
								|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_SCC))
								|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_CC))
								|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_EC))
								|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_G))
								|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_MH))
								|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_ML))
								|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_M)))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_PW))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_PL))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_REB))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_SPC))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_S))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_T))) {

					sanctionFee = calculateConstantFee(paramMap, 30);

				} else if ((subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_AB))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_GO))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_LSGO))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_P))) {
					sanctionFee = calculateConstantFee(paramMap, 10);
				} else if ((subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_SWC))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_CI))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_D))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_YC))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_DC))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_GSGH))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_RT))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_HC))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_H))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_L))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_MTH))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_MB))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_NH))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_PLY))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_RC))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_VHAB))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_RTI))) {
					sanctionFee = calculateConstantFee(paramMap, 30);
				} else if ((subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_PS))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_FS))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_J))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_PO))) {
					sanctionFee = calculateConstantFee(paramMap, 10);
				}
			}
		}
		return sanctionFee;
	}

	private BigDecimal calculateSanctionFeeForAgricultureOccupancy(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);

		BigDecimal sanctionFee = BigDecimal.ZERO;

		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.H))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				sanctionFee = calculateConstantFee(paramMap, 10);
			}
		}
		return sanctionFee;
	}

	private BigDecimal calculateSanctionFeeForTransportationOccupancy(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);

		BigDecimal sanctionFee = BigDecimal.ZERO;
		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.G))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				sanctionFee = calculateConstantFee(paramMap, 10);
			}
		}
		return sanctionFee;
	}

	private BigDecimal calculateSanctionFeeForEducationOccupancy(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);

		BigDecimal sanctionFee = BigDecimal.ZERO;
		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.F))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				sanctionFee = calculateConstantFee(paramMap, 30);
			}
		}
		return sanctionFee;
	}

	private BigDecimal calculateSanctionFeeForPublicUtilityOccupancy(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);

		BigDecimal sanctionFee = BigDecimal.ZERO;
		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.D))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				sanctionFee = calculateConstantFee(paramMap, 10);
			}
		}
		return sanctionFee;
	}

	private BigDecimal calculateSanctionFeeForIndustrialZoneOccupancy(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get("OCCUPANCY_TYPE");

		BigDecimal sanctionFee = BigDecimal.ZERO;
		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.E))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				sanctionFee = calculateConstantFee(paramMap, 60);
			}
		}
		return sanctionFee;
	}

	private BigDecimal calculateSanctionFeeForCommercialOccupancy(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);

		BigDecimal sanctionFee = BigDecimal.ZERO;
		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.B))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				sanctionFee = calculateConstantFee(paramMap, 60);
			}
		}
		return sanctionFee;
	}

	private BigDecimal calculateSanctionFeeForResidentialOccupancy(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);
		String subOccupancyType = (String) paramMap.get(BPACalculatorConstants.SUB_OCCUPANCY_TYPE);

		BigDecimal sanctionFee = BigDecimal.ZERO;
		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.A) && StringUtils.hasText(subOccupancyType))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_P))
						|| (occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_S))
						|| occupancyType.equalsIgnoreCase(BPACalculatorConstants.A_R)) {
					sanctionFee = calculateConstantFee(paramMap, 15);

				} else {
					sanctionFee = calculateConstantFee(paramMap, 50);
				}

			}

		}
		return sanctionFee;
	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param riskType
	 * @param occupancyType
	 * @param subOccupancyType
	 * @param plotArea
	 * @param totalBuitUpArea
	 * @return
	 */
	private BigDecimal calculateTotalScrutinyFee(Map<String, Object> paramMap) {

		BigDecimal calculatedTotalScrutinyFee = BigDecimal.ZERO;

		BigDecimal feeForDevelopmentOfLand = calculateFeeForDevelopmentOfLand(paramMap);
		BigDecimal feeForBuildingOperation = calculateFeeForBuildingOperation(paramMap);
		calculatedTotalScrutinyFee = (calculatedTotalScrutinyFee.add(feeForDevelopmentOfLand)
				.add(feeForBuildingOperation)).setScale(2, BigDecimal.ROUND_UP);
		return calculatedTotalScrutinyFee;
	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param riskType
	 * @param plotArea
	 * @return
	 */
	private BigDecimal calculateFeeForDevelopmentOfLand(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String riskType = (String) paramMap.get(BPACalculatorConstants.RISK_TYPE);
		Double plotArea = (Double) paramMap.get(BPACalculatorConstants.PLOT_AREA);

		BigDecimal feeForDevelopmentOfLand = BigDecimal.ZERO;

		if ((StringUtils.hasText(applicationType)
				&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
				&& (StringUtils.hasText(serviceType)
						&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
			if (((StringUtils.hasText(riskType)) && !(riskType.equalsIgnoreCase("LOW"))) && (null != plotArea)) {

				feeForDevelopmentOfLand = calculateConstantFee(paramMap, 5);
			}

		}
		return feeForDevelopmentOfLand;

	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param occupancyType
	 * @param subOccupancyType
	 * @param totalBuitUpArea
	 * @return
	 */
	private BigDecimal calculateFeeForBuildingOperation(Map<String, Object> paramMap) {
		Double totalBuitUpArea = (Double) paramMap.get(BPACalculatorConstants.BUILTUP_AREA);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);
		BigDecimal feeForBuildingOperation = BigDecimal.ZERO;
		if (totalBuitUpArea != null) {
			if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.A))) {
				feeForBuildingOperation = calculateBuildingOperationFeeForResidentialOccupancy(paramMap);
			}
			if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.B))) {
				feeForBuildingOperation = calculateBuildingOperationFeeForCommercialOccupancy(paramMap);
			}
			if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.C))) {
				feeForBuildingOperation = calculateBuildingOperationFeeForPublicSemiPublicInstitutionalOccupancy(
						paramMap);
			}
			if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.D))) {
				feeForBuildingOperation = calculateBuildingOperationFeeForPublicUtilityOccupancy(paramMap);
			}
			if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.E))) {
				feeForBuildingOperation = calculateBuildingOperationFeeForIndustrialZoneOccupancy(paramMap);
			}
			if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.F))) {
				feeForBuildingOperation = calculateBuildingOperationFeeForEducationOccupancy(paramMap);
			}
			if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.G))) {
				feeForBuildingOperation = calculateBuildingOperationFeeForTransportationOccupancy(paramMap);
			}
			if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.H))) {
				feeForBuildingOperation = calculateBuildingOperationFeeForAgricultureOccupancy(paramMap);
			}

		}
		return feeForBuildingOperation;
	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param occupancyType
	 * @param totalBuitUpArea
	 * @param feeForBuildingOperation
	 * @return
	 */
	private BigDecimal calculateBuildingOperationFeeForAgricultureOccupancy(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);
		Double totalBuitUpArea = (Double) paramMap.get(BPACalculatorConstants.BUILTUP_AREA);

		BigDecimal feeForBuildingOperation = BigDecimal.ZERO;
		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.H))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				feeForBuildingOperation = calculateVariableFee1(totalBuitUpArea);
			}
		}
		return feeForBuildingOperation;
	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param occupancyType
	 * @param totalBuitUpArea
	 * @param feeForBuildingOperation
	 * @return
	 */
	private BigDecimal calculateBuildingOperationFeeForTransportationOccupancy(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);

		BigDecimal feeForBuildingOperation = BigDecimal.ZERO;
		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.G))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				feeForBuildingOperation = calculateConstantFee(paramMap, 5);
			}
		}
		return feeForBuildingOperation;
	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param riskType
	 * @param totalBuitUpArea
	 * @param multiplicationFactor
	 * @return
	 */
	private BigDecimal calculateConstantFee(Map<String, Object> paramMap, int multiplicationFactor) {
		Double totalBuitUpArea = (Double) paramMap.get(BPACalculatorConstants.BUILTUP_AREA);

		BigDecimal totalAmount = BigDecimal.ZERO;
		totalAmount = (BigDecimal.valueOf(totalBuitUpArea).multiply(BigDecimal.valueOf(multiplicationFactor)))
				.setScale(2, BigDecimal.ROUND_UP);
		return totalAmount;
	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param occupancyType
	 * @param totalBuitUpArea
	 * @param feeForBuildingOperation
	 * @return
	 */
	private BigDecimal calculateBuildingOperationFeeForEducationOccupancy(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);

		BigDecimal feeForBuildingOperation = BigDecimal.ZERO;
		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.F))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				feeForBuildingOperation = calculateConstantFee(paramMap, 5);
			}
		}
		return feeForBuildingOperation;
	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param occupancyType
	 * @param totalBuitUpArea
	 * @param feeForBuildingOperation
	 * @return
	 */
	private BigDecimal calculateBuildingOperationFeeForIndustrialZoneOccupancy(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);
		Double totalBuitUpArea = (Double) paramMap.get(BPACalculatorConstants.BUILTUP_AREA);

		BigDecimal feeForBuildingOperation = BigDecimal.ZERO;
		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.E))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				feeForBuildingOperation = calculateVariableFee3(totalBuitUpArea);
			}
		}
		return feeForBuildingOperation;
	}

	/**
	 * @param totalBuitUpArea
	 * @param feeForBuildingOperation
	 * @return
	 */
	private BigDecimal calculateVariableFee3(Double totalBuitUpArea) {
		BigDecimal amount = BigDecimal.ZERO;
		if (totalBuitUpArea <= 100) {
			amount = BigDecimal.valueOf(1500);
		} else if (totalBuitUpArea <= 300) {
			amount = (FIFTEEN_HUNDRED.add(TWENTY_FIVE.multiply(BigDecimal.valueOf(totalBuitUpArea).subtract(HUNDRED))))
					.setScale(2, BigDecimal.ROUND_UP);
		} else if (totalBuitUpArea > 300) {
			amount = (FIFTEEN_HUNDRED.add(TWENTY_FIVE.multiply(TWO_HUNDRED))
					.add(FIFTEEN.multiply(BigDecimal.valueOf(totalBuitUpArea).subtract(THREE_HUNDRED)))).setScale(2,
							BigDecimal.ROUND_UP);
		}
		return amount;
	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param occupancyType
	 * @param totalBuitUpArea
	 * @param feeForBuildingOperation
	 * @return
	 */
	private BigDecimal calculateBuildingOperationFeeForPublicUtilityOccupancy(Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);

		BigDecimal feeForBuildingOperation = BigDecimal.ZERO;
		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.D))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				feeForBuildingOperation = calculateConstantFee(paramMap, 5);

			}
		}
		return feeForBuildingOperation;
	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param occupancyType
	 * @param subOccupancyType
	 * @param totalBuitUpArea
	 * @param feeForBuildingOperation
	 * @return
	 */
	private BigDecimal calculateBuildingOperationFeeForPublicSemiPublicInstitutionalOccupancy(
			Map<String, Object> paramMap) {
		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);
		String subOccupancyType = (String) paramMap.get(BPACalculatorConstants.SUB_OCCUPANCY_TYPE);
		Double totalBuitUpArea = (Double) paramMap.get(BPACalculatorConstants.BUILTUP_AREA);

		BigDecimal feeForBuildingOperation = BigDecimal.ZERO;
		if (((occupancyType.equalsIgnoreCase(BPACalculatorConstants.C))) && (StringUtils.hasText(subOccupancyType))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				if ((subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_A))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_B))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_C))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_CL))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_MP))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_CH))) {
					feeForBuildingOperation = calculateVariableFee2(totalBuitUpArea);

				} else if ((subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_O))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_OAH))) {
					feeForBuildingOperation = calculateConstantFee(paramMap, 5);

				} else if ((subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_SC))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_C1H))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_C2H))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_SCC))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_CC))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_EC))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_G))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_MH))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_ML))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_M))) {
					feeForBuildingOperation = calculateVariableFee2(totalBuitUpArea);

				} else if ((subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_PW))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_PL))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_REB))) {
					feeForBuildingOperation = calculateConstantFee(paramMap, 5);

				} else if ((subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_SPC))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_S))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_T))) {
					feeForBuildingOperation = calculateVariableFee2(totalBuitUpArea);

				} else if ((subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_AB))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_GO))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_LSGO))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_P))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_RB))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_SWC))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_CI))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_D))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_YC))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_DC))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_GSGH))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_RT))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_HC))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_H))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_L))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_MTH))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_MB))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_NH))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_PLY))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_RC))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_VHAB))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_RTI))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_PS))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_FS))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_J))
						|| (subOccupancyType.equalsIgnoreCase(BPACalculatorConstants.C_PO))) {

					feeForBuildingOperation = calculateConstantFee(paramMap, 5);

				}
			}

		}
		return feeForBuildingOperation;
	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param occupancyType
	 * @param totalBuitUpArea
	 * @param feeForBuildingOperation
	 * @return
	 */
	private BigDecimal calculateBuildingOperationFeeForCommercialOccupancy(Map<String, Object> paramMap) {

		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);
		Double totalBuitUpArea = (Double) paramMap.get(BPACalculatorConstants.BUILTUP_AREA);

		BigDecimal feeForBuildingOperation = BigDecimal.ZERO;
		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.B))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				feeForBuildingOperation = calculateVariableFee2(totalBuitUpArea);
			}

		}
		return feeForBuildingOperation;
	}

	/**
	 * @param totalBuitUpArea
	 * @param feeForBuildingOperation
	 * @return
	 */
	private BigDecimal calculateVariableFee2(Double totalBuitUpArea) {

		BigDecimal amount = BigDecimal.ZERO;
		if (totalBuitUpArea <= 20) {
			amount = BigDecimal.valueOf(500);
		} else if (totalBuitUpArea <= 50) {
			amount = (FIVE_HUNDRED.add(FIFTY.multiply(BigDecimal.valueOf(totalBuitUpArea).subtract(TWENTY))))
					.setScale(2, BigDecimal.ROUND_UP);
		} else if (totalBuitUpArea > 50) {
			amount = (FIVE_HUNDRED.add(FIFTY.multiply(THIRTY))
					.add(TWENTY.multiply(BigDecimal.valueOf(totalBuitUpArea).subtract(FIFTY)))).setScale(2,
							BigDecimal.ROUND_UP);
		}
		return amount;

	}

	/**
	 * @param applicationType
	 * @param serviceType
	 * @param occupancyType
	 * @param totalBuitUpArea
	 * @param feeForBuildingOperation
	 * @return
	 */
	private BigDecimal calculateBuildingOperationFeeForResidentialOccupancy(Map<String, Object> paramMap) {

		String applicationType = (String) paramMap.get(BPACalculatorConstants.APPLICATION_TYPE);
		String serviceType = (String) paramMap.get(BPACalculatorConstants.SERVICE_TYPE);
		String occupancyType = (String) paramMap.get(BPACalculatorConstants.OCCUPANCY_TYPE);
		Double totalBuitUpArea = (Double) paramMap.get(BPACalculatorConstants.BUILTUP_AREA);

		BigDecimal feeForBuildingOperation = BigDecimal.ZERO;
		if ((occupancyType.equalsIgnoreCase(BPACalculatorConstants.A))) {
			if ((StringUtils.hasText(applicationType)
					&& applicationType.equalsIgnoreCase(BPACalculatorConstants.BUILDING_PLAN_SCRUTINY))
					&& (StringUtils.hasText(serviceType)
							&& serviceType.equalsIgnoreCase(BPACalculatorConstants.NEW_CONSTRUCTION))) {
				feeForBuildingOperation = calculateVariableFee1(totalBuitUpArea);
			}

		}
		return feeForBuildingOperation;
	}

	/**
	 * @param totalBuitUpArea
	 * @param feeForBuildingOperation
	 * @return
	 */
	private BigDecimal calculateVariableFee1(Double totalBuitUpArea) {

		BigDecimal amount = BigDecimal.ZERO;
		if (totalBuitUpArea <= 100) {
			amount = TWO_HUNDRED_FIFTY;
		} else if (totalBuitUpArea <= 300) {
			amount = (TWO_HUNDRED_FIFTY.add(FIFTEEN.multiply(BigDecimal.valueOf(totalBuitUpArea).subtract(HUNDRED))))
					.setScale(2, BigDecimal.ROUND_UP);
		} else if (totalBuitUpArea > 300) {
			amount = (TWO_HUNDRED_FIFTY.add(FIFTEEN.multiply(TWO_HUNDRED))
					.add(TEN.multiply(BigDecimal.valueOf(totalBuitUpArea).subtract(THREE_HUNDRED)))).setScale(2,
							BigDecimal.ROUND_UP);
		}
		return amount;

	}

}
