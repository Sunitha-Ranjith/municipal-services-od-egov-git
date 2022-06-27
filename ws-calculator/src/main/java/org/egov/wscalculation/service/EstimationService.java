package org.egov.wscalculation.service;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.egov.common.contract.request.RequestInfo;
import org.egov.tracer.model.CustomException;
import org.egov.wscalculation.config.WSCalculationConfiguration;
import org.egov.wscalculation.constants.WSCalculationConstant;
import org.egov.wscalculation.producer.WSCalculationProducer;
import org.egov.wscalculation.repository.WSCalculationDao;
import org.egov.wscalculation.util.CalculatorUtil;
import org.egov.wscalculation.util.WSCalculationUtil;
import org.egov.wscalculation.util.WaterCessUtil;
import org.egov.wscalculation.web.models.AnnualAdvance;
import org.egov.wscalculation.web.models.AnnualPaymentDetails;
import org.egov.wscalculation.web.models.AuditDetails;
import org.egov.wscalculation.web.models.BillAccountDetail;
import org.egov.wscalculation.web.models.BillResponse;
import org.egov.wscalculation.web.models.BillingSlab;
import org.egov.wscalculation.web.models.CalculationCriteria;
import org.egov.wscalculation.web.models.Installments;
import org.egov.wscalculation.web.models.InstallmentsRequest;
import org.egov.wscalculation.web.models.RequestInfoWrapper;
import org.egov.wscalculation.web.models.RoadCuttingInfo;
import org.egov.wscalculation.web.models.SearchCriteria;
import org.egov.wscalculation.web.models.Slab;
import org.egov.wscalculation.web.models.TaxHeadEstimate;
import org.egov.wscalculation.web.models.WaterConnection;
import org.egov.wscalculation.web.models.WaterConnectionRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;

import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

@Service
@Slf4j
public class EstimationService {

	@Autowired
	private WaterCessUtil waterCessUtil;
	
	@Autowired
	private CalculatorUtil calculatorUtil;

	@Autowired
	private ObjectMapper mapper;
	
	@Autowired
	private WSCalculationUtil wSCalculationUtil;
	
	@Autowired
	private MasterDataService mDataService;

	@Autowired
	private PayService payService;
	
	@Autowired
	private WSCalculationConfiguration configs;
	
	@Autowired
	private MeterService meterService;
	
	@Autowired 
	private WSCalculationProducer wsCalculationProducer;

	 @Autowired
	 private WSCalculationDao waterCalculatorDao;
	 
	@Autowired
	private DemandService demandService;

	@Autowired
	private AnnualAdvanceService annualAdvanceService;
	
	private static BigDecimal OWNERSHIP_CHANGE_FEE = BigDecimal.valueOf(60);
	private static BigDecimal TenPercent = BigDecimal.valueOf(0.1);
	private static BigDecimal FivePercent = BigDecimal.valueOf(0.05);

	/**
	 * Generates a List of Tax head estimates with tax head code, tax head
	 * category and the amount to be collected for the key.
	 *
	 * @param criteria
	 *            criteria based on which calculation will be done.
	 * @param requestInfo
	 *            request info from incoming request.
	 * @return Map<String, Double>
	 */
	@SuppressWarnings("rawtypes")
	public Map<String, List> getEstimationMap(CalculationCriteria criteria, RequestInfo requestInfo,
			Map<String, Object> masterData) {
//		String tenantId = requestInfo.getUserInfo().getTenantId();
		String tenantId = criteria.getTenantId();
		if (criteria.getWaterConnection() == null && !StringUtils.isEmpty(criteria.getConnectionNo())) {
			List<WaterConnection> waterConnectionList = calculatorUtil.getWaterConnection(requestInfo, criteria.getConnectionNo(), tenantId);
			WaterConnection waterConnection = calculatorUtil.getWaterConnectionObject(waterConnectionList);
			criteria.setWaterConnection(waterConnection);
		}
		if (criteria.getWaterConnection() == null || StringUtils.isEmpty(criteria.getConnectionNo())) {
			StringBuilder builder = new StringBuilder();
			builder.append("Water Connection are not present for ")
					.append(StringUtils.isEmpty(criteria.getConnectionNo()) ? "" : criteria.getConnectionNo())
					.append(" connection no");
			throw new CustomException("WATER_CONNECTION_NOT_FOUND", builder.toString());
		}
		
		Map<String, JSONArray> billingSlabMaster = new HashMap<>();
		Map<String, JSONArray> timeBasedExemptionMasterMap = new HashMap<>();
		ArrayList<String> billingSlabIds = new ArrayList<>();
		billingSlabMaster.put(WSCalculationConstant.WC_BILLING_SLAB_MASTER,
				(JSONArray) masterData.get(WSCalculationConstant.WC_BILLING_SLAB_MASTER));
		billingSlabMaster.put(WSCalculationConstant.CALCULATION_ATTRIBUTE_CONST,
				(JSONArray) masterData.get(WSCalculationConstant.CALCULATION_ATTRIBUTE_CONST));
		timeBasedExemptionMasterMap.put(WSCalculationConstant.WC_WATER_CESS_MASTER,
				(JSONArray) (masterData.getOrDefault(WSCalculationConstant.WC_WATER_CESS_MASTER, null)));
		
		mDataService.setWaterConnectionMasterValues(requestInfo, criteria.getTenantId(), billingSlabMaster,
				timeBasedExemptionMasterMap);
		
		ArrayList<?> billingFrequencyMap = (ArrayList<?>) masterData
				.get(WSCalculationConstant.Billing_Period_Master);
		mDataService.enrichBillingPeriod(criteria, billingFrequencyMap, masterData, criteria.getWaterConnection().getConnectionType());
		// mDataService.setWaterConnectionMasterValues(requestInfo, tenantId,
		// billingSlabMaster,
		// timeBasedExemptionMasterMap);
		// BigDecimal taxAmt = getWaterEstimationCharge(criteria.getWaterConnection(), criteria, billingSlabMaster, billingSlabIds,
		// 		requestInfo);
		BigDecimal waterTaxAmt = getWaterEstimationChargeV2(criteria.getWaterConnection(), criteria, requestInfo, masterData);
		BigDecimal sewerageTaxAmt = getSewerageEstimationChargeV2(criteria.getWaterConnection(), criteria, requestInfo);
		List<TaxHeadEstimate> taxHeadEstimates = getEstimatesForTax(waterTaxAmt, sewerageTaxAmt, criteria,
				timeBasedExemptionMasterMap, RequestInfoWrapper.builder().requestInfo(requestInfo).build(), masterData);

		Map<String, List> estimatesAndBillingSlabs = new HashMap<>();
		estimatesAndBillingSlabs.put("estimates", taxHeadEstimates);
		// Billing slab id
		estimatesAndBillingSlabs.put("billingSlabIds", billingSlabIds);
		return estimatesAndBillingSlabs;
	}

	private BigDecimal getWaterEstimationChargeV2(WaterConnection waterConnection, CalculationCriteria criteria,
			RequestInfo requestInfo, Map<String, Object> masterData) {
		BigDecimal waterCharges = BigDecimal.ZERO;
		if(WSCalculationConstant.MDMS_SEWERAGE_CONNECTION.equalsIgnoreCase(criteria.getWaterConnection().getConnectionFacility())) {
			return waterCharges;
		}
		//
		String usageType = waterConnection.getUsageCategory();
		//
		
		
		if(criteria.getWaterConnection().getConnectionType().equals(WSCalculationConstant.meteredConnectionType)) {
			LinkedHashMap additionalDetails = (LinkedHashMap)criteria.getWaterConnection().getAdditionalDetails();
			long ratio = 1;
			if(additionalDetails.containsKey("meterReadingRatio")) {
				String meterReadingRatio = additionalDetails.get("meterReadingRatio").toString();
				if(!StringUtils.isEmpty(meterReadingRatio)) {
					ratio = Long.parseLong(meterReadingRatio.split(":")[1]);
				}
			}
			Double totalUnit = criteria.getCurrentReading() - criteria.getLastReading();
			BigDecimal rate = getMeteredRate(criteria, usageType);
			waterCharges = rate.multiply(BigDecimal.valueOf(totalUnit)).multiply(BigDecimal.valueOf(ratio));
		} else if(criteria.getWaterConnection().getConnectionType().equals(WSCalculationConstant.nonMeterdConnection)) {
			String isVolumetricConnection = WSCalculationConstant.NO;
			boolean isMigratedConnection = true;
			String isDailyConsumption = WSCalculationConstant.NO;
			String volumetricConsumption = "0";
			if(StringUtils.isEmpty(criteria.getWaterConnection().getOldConnectionNo())) {
				isMigratedConnection = false;
			}
			LinkedHashMap additionalDetails = (LinkedHashMap)criteria.getWaterConnection().getAdditionalDetails();
			if(additionalDetails.containsKey(WSCalculationConstant.IS_VOLUMETRIC_CONNECTION)) {
				isVolumetricConnection = additionalDetails.get(WSCalculationConstant.IS_VOLUMETRIC_CONNECTION).toString();
			}
			if(additionalDetails.containsKey(WSCalculationConstant.IS_DAILY_CONSUMPTION)) {
				isDailyConsumption = additionalDetails.get(WSCalculationConstant.IS_DAILY_CONSUMPTION).toString();
			}
			if(additionalDetails.containsKey(WSCalculationConstant.VOLUMETRIC_CONSUMPTION)) {
				volumetricConsumption = additionalDetails.get(WSCalculationConstant.VOLUMETRIC_CONSUMPTION).toString();
			}
			if(isVolumetricConnection != null && WSCalculationConstant.YES.equalsIgnoreCase(isVolumetricConnection)) {
				BigDecimal volumetricWaterCharge = BigDecimal.ZERO;
				if(isMigratedConnection) {
					if(additionalDetails.containsKey(WSCalculationConstant.VOLUMETRIC_WATER_CHARGE)) {
						String amount = additionalDetails.get(WSCalculationConstant.VOLUMETRIC_WATER_CHARGE).toString();
						if(StringUtils.hasText(amount.trim())) {
							volumetricWaterCharge = new BigDecimal(amount.trim());
						}
					}
				} else {
					BigDecimal rate = getMeteredRate(criteria, usageType);
					if(WSCalculationConstant.YES.equalsIgnoreCase(isDailyConsumption)) {
						BigDecimal maxDays = getDaysInMonth(masterData.get(WSCalculationConstant.BILLING_PERIOD));
						BigDecimal volumetricConsumptionKL = new BigDecimal(volumetricConsumption);
						volumetricWaterCharge = volumetricConsumptionKL.multiply(maxDays).multiply(rate).setScale(2, RoundingMode.HALF_UP);
					} else {
						BigDecimal volumetricConsumptionKL = new BigDecimal(volumetricConsumption);
						volumetricWaterCharge = volumetricConsumptionKL.multiply(rate).setScale(2, RoundingMode.HALF_UP);
					}
				}
				return volumetricWaterCharge;
			}
			
			if(criteria.getWaterConnection().getConnectionCategory().equalsIgnoreCase("Permanent")) {
				if(WSCalculationConstant.WS_UC_DOMESTIC.equalsIgnoreCase(usageType)) {
					waterCharges = BigDecimal.valueOf(106);
					if(criteria.getWaterConnection().getNoOfTaps() > 2) {
						waterCharges = waterCharges.add(BigDecimal.valueOf(35.18).multiply(BigDecimal.valueOf(criteria.getWaterConnection().getNoOfTaps() - 2)));
					}
				} else if(WSCalculationConstant.WS_UC_BPL.equalsIgnoreCase(usageType)) {
					waterCharges = BigDecimal.valueOf(56);
				}
			} else if(criteria.getWaterConnection().getConnectionCategory().equalsIgnoreCase("Temporary")) {
				if(WSCalculationConstant.WS_UC_TWSFC.equalsIgnoreCase(usageType)) {
					waterCharges = BigDecimal.valueOf(218);
				} else if(WSCalculationConstant.WS_UC_BPL.equalsIgnoreCase(usageType)) {
					waterCharges = BigDecimal.valueOf(53);
				} else if(WSCalculationConstant.WS_UC_ROADSIDEEATERS.equalsIgnoreCase(usageType)) {
					waterCharges = BigDecimal.valueOf(360);
				} else if(WSCalculationConstant.WS_UC_STAND_POST_MUNICIPALITY.equalsIgnoreCase(usageType)) {
					waterCharges = BigDecimal.valueOf(218);
				}
			}
		}
		return waterCharges;
	}
	
	private BigDecimal getSewerageEstimationChargeV2(WaterConnection waterConnection, CalculationCriteria criteria,
			RequestInfo requestInfo) {

		BigDecimal sewerageCharge = BigDecimal.ZERO;
		if(WSCalculationConstant.MDMS_WATER_CONNECTION.equalsIgnoreCase(criteria.getWaterConnection().getConnectionFacility())) {
			return sewerageCharge;
		}
		
		BigDecimal monthsApplicable = BigDecimal.ONE;
		if(criteria.getWaterConnection().getConnectionType().equals(WSCalculationConstant.meteredConnectionType)
				&& WSCalculationConstant.MDMS_WATER_SEWERAGE_CONNECTION.equalsIgnoreCase(criteria.getWaterConnection().getConnectionFacility())) {
			int swChargeApplicableMonth = getMonthDifference(criteria.getFrom(), criteria.getTo());
			monthsApplicable = BigDecimal.valueOf(swChargeApplicableMonth);
		}
		
		log.info("Generate Sewerage demand with migrated value: " + configs.isSwDemandMigratedAmountEnabled());
		if(StringUtils.hasText(criteria.getWaterConnection().getOldConnectionNo()) && configs.isSwDemandMigratedAmountEnabled()) {
			LinkedHashMap additionalDetails = (LinkedHashMap)criteria.getWaterConnection().getAdditionalDetails();
			BigDecimal migratedSewerageFee = BigDecimal.ZERO;
			if(additionalDetails.containsKey("migratedSewerageFee")) {
				migratedSewerageFee = StringUtils.isEmpty(additionalDetails.get("migratedSewerageFee")) ? BigDecimal.ZERO : new BigDecimal(additionalDetails.get("migratedSewerageFee").toString());
//				log.info("Migrated sewerage amount: " + migratedSewerageFee.toString());
			}
			return migratedSewerageFee.multiply(monthsApplicable);
		}

		String usageCategory = criteria.getWaterConnection().getUsageCategory();

		if (criteria.getWaterConnection().getConnectionCategory().equalsIgnoreCase("Permanent")) {
			if (WSCalculationConstant.WS_UC_INSTITUTIONAL.equalsIgnoreCase(usageCategory)) {
				if (criteria.getWaterConnection().getNoOfWaterClosets().compareTo(4) <= 0) {
					sewerageCharge = BigDecimal.valueOf(100);
				} else {
					sewerageCharge = BigDecimal.valueOf(200);
				}
			} else if (WSCalculationConstant.WS_UC_DOMESTIC.equalsIgnoreCase(usageCategory)) {
				if (criteria.getWaterConnection().getNoOfFlats() > 0) {
					sewerageCharge = calculateDiameterFixedCharge(criteria.getWaterConnection().getAdditionalDetails());
				} else {
					sewerageCharge = BigDecimal.valueOf(50);
				}
			} else if (WSCalculationConstant.WS_UC_BPL.equalsIgnoreCase(usageCategory)) {
				sewerageCharge = BigDecimal.valueOf(50);
			} else if (WSCalculationConstant.WS_UC_COMMERCIAL.equalsIgnoreCase(usageCategory)
							|| WSCalculationConstant.WS_UC_INDUSTRIAL.equalsIgnoreCase(usageCategory)) {
				sewerageCharge = calculateDiameterFixedCharge(criteria.getWaterConnection().getAdditionalDetails());
			}
		}
		
		return sewerageCharge.multiply(monthsApplicable);

	}
	
	private int getMonthDifference(Long fromLongDate, Long toLongDate) {
		Calendar lastMeterReadingDate = calculatorUtil.getCalendar(fromLongDate);
		Calendar currentMeterReadingDate = calculatorUtil.getCalendar(toLongDate);
		LocalDate fromDate = LocalDateTime.ofInstant(lastMeterReadingDate.toInstant(), lastMeterReadingDate.getTimeZone().toZoneId()).toLocalDate();
		LocalDate toDate = LocalDateTime.ofInstant(currentMeterReadingDate.toInstant(), currentMeterReadingDate.getTimeZone().toZoneId()).toLocalDate();
		Period diff = Period.between(fromDate.withDayOfMonth(1), toDate.withDayOfMonth(1));
		return diff.getMonths();
	}
	

	/**
	 * 
	 * @param waterCharge WaterCharge amount
	 * @param connection - Connection Object
	 * @param timeBasedExemptionsMasterMap List of Exemptions for the connection
	 * @param requestInfoWrapper - RequestInfo Wrapper object
	 * @return - Returns list of TaxHeadEstimates
	 */
	private List<TaxHeadEstimate> getEstimatesForTax(BigDecimal waterCharge, BigDecimal SewerageCharge,
			CalculationCriteria criteria,
			Map<String, JSONArray> timeBasedExemptionsMasterMap, RequestInfoWrapper requestInfoWrapper,
			Map<String, Object> masterData) {
		WaterConnection connection = criteria.getWaterConnection();
		
		
		@SuppressWarnings("unchecked")
		Map<String, Object> financialYearMaster =  (Map<String, Object>) masterData
				.get(WSCalculationConstant.BILLING_PERIOD);
		Long toDate = (Long) financialYearMaster.get(WSCalculationConstant.ENDING_DATE_APPLICABLES);
		Calendar billingPeriodTo = calculatorUtil.getCalendar(toDate);
		
		List<TaxHeadEstimate> estimates = new ArrayList<>();
		
		if(WSCalculationConstant.MDMS_WATER_CONNECTION.equalsIgnoreCase(connection.getConnectionFacility())
				|| WSCalculationConstant.MDMS_WATER_SEWERAGE_CONNECTION.equalsIgnoreCase(connection.getConnectionFacility())) {
			// water_charge
			estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_CHARGE)
					.estimateAmount(waterCharge.setScale(2, 2)).build());
		}

		if(((WSCalculationConstant.meteredConnectionType.equalsIgnoreCase(criteria.getWaterConnection().getConnectionType())
				&& !billingPeriodTo.getTime().before(configs.getSwApplicableForMeter()))
			||(WSCalculationConstant.nonMeterdConnection.equalsIgnoreCase(criteria.getWaterConnection().getConnectionType())
						&& !billingPeriodTo.getTime().before(configs.getSwApplicableForNonMeter())))
			&& (WSCalculationConstant.MDMS_SEWERAGE_CONNECTION.equalsIgnoreCase(connection.getConnectionFacility())
					|| WSCalculationConstant.MDMS_WATER_SEWERAGE_CONNECTION.equalsIgnoreCase(connection.getConnectionFacility()))) {
				// sewerage_charge
				estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.SW_CHARGE)
						.estimateAmount(SewerageCharge.setScale(2, 2)).build());
		} else if(WSCalculationConstant.nonMeterdConnection.equalsIgnoreCase(criteria.getWaterConnection().getConnectionType())
					&& WSCalculationConstant.MDMS_SEWERAGE_CONNECTION.equalsIgnoreCase(connection.getConnectionFacility())
					&& billingPeriodTo.getTime().before(configs.getSwApplicableForNonMeter())){
			SewerageCharge = BigDecimal.ZERO;
			estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.SW_CHARGE)
					.estimateAmount(SewerageCharge.setScale(2, 2)).build());
		} else {
			SewerageCharge = BigDecimal.ZERO;
		}

		BigDecimal totalCharge = waterCharge.add(SewerageCharge);

		// water timebase Rebate
//		BigDecimal timeBaseRebate = payService.getApplicableRebateForInitialDemand(totalCharge.setScale(2, 2), getAssessmentYear(), timeBasedExemptionsMasterMap.get(WSCalculationConstant.WC_REBATE_MASTER));
//		if(timeBaseRebate.compareTo(BigDecimal.ZERO) > 0) {
//			estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_TIME_REBATE)
//					.estimateAmount(timeBaseRebate.setScale(2, 2).negate()).build());
//		}
		
		// water Special Rebate
//		if(isSpecialRebateApplicableForMonth(criteria, masterData)) {
//			BigDecimal specialRebate = payService.getApplicableSpecialRebate(totalCharge.setScale(2, 2), getAssessmentYear(), timeBasedExemptionsMasterMap.get(WSCalculationConstant.WC_REBATE_MASTER));
//			if(specialRebate.compareTo(BigDecimal.ZERO) > 0) {
//				estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_SPECIAL_REBATE)
//						.estimateAmount(specialRebate.setScale(2, 2).negate()).build());
//			}
//		}
		
		// Sewerage arrear fee
		if(configs.isSwArrearDemandEnabled()) {
			BigDecimal swArrearAmount = BigDecimal.ZERO;
			LinkedHashMap additionalDetails = (LinkedHashMap)connection.getAdditionalDetails();
			BigDecimal migratedSewerageFee = BigDecimal.ZERO;
			if(additionalDetails.containsKey("migratedSewerageFee")) {
				migratedSewerageFee = new BigDecimal(additionalDetails.get("migratedSewerageFee").toString());
				log.info("Migrated sewerage amount: " + migratedSewerageFee.toString());
			}
			if(WSCalculationConstant.meteredConnectionType.equalsIgnoreCase(criteria.getWaterConnection().getConnectionType())
					&& billingPeriodTo.get(Calendar.MONTH)+1==configs.getSwArrearBillingMonthMeter()
					&& billingPeriodTo.get(Calendar.YEAR)==configs.getSwArrearBillingYearMeter()) {
				int swDemandMonth = configs.getSwArrearMonthCountForMeter();
				int lastSwChargeApplicableFor = getMonthDifference(criteria.getFrom(), criteria.getTo());
				int swArrearApplicaleMonth = swDemandMonth-lastSwChargeApplicableFor+1;
				log.info("Generating sewerage arrear bill for " + swArrearApplicaleMonth + " months");
				swArrearAmount = migratedSewerageFee.multiply(BigDecimal.valueOf(swArrearApplicaleMonth)).setScale(2, 2);
			} else if(WSCalculationConstant.nonMeterdConnection.equalsIgnoreCase(criteria.getWaterConnection().getConnectionType())
					&& billingPeriodTo.get(Calendar.MONTH)+1==configs.getSwArrearBillingMonthNonMeter()
					&& billingPeriodTo.get(Calendar.YEAR)==configs.getSwArrearBillingYearNonMeter()) {
				int swDemandMonth = configs.getSwArrearMonthCountForNonMeter();
				log.info("Generating sewerage arrear bill for " + swDemandMonth + " months");
				swArrearAmount = migratedSewerageFee.multiply(BigDecimal.valueOf(swDemandMonth)).setScale(2, 2);
			}
			
			if(swArrearAmount.compareTo(BigDecimal.ZERO) > 0) {
				estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.SW_ADHOC_CHARGE)
						.estimateAmount(swArrearAmount.setScale(2, 2)).build());
			}
			
			// Sewerage Special Rebate
			if(swArrearAmount.compareTo(BigDecimal.ZERO) > 0 && configs.isSwSpecialRebateApplicable()) {
				BigDecimal swSpecialRebate = BigDecimal.ZERO;
				swSpecialRebate = swArrearAmount.multiply(BigDecimal.valueOf(0.02)).setScale(2, 2);
				if(swSpecialRebate.compareTo(BigDecimal.ZERO) > 0) {
					estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.SW_SPECIAL_REBATE)
							.estimateAmount(swSpecialRebate.setScale(2, 2).negate()).build());
				}
			}
		}
		
		// Water_cess
		if (timeBasedExemptionsMasterMap.get(WSCalculationConstant.WC_WATER_CESS_MASTER) != null
				&& (WSCalculationConstant.MDMS_WATER_CONNECTION.equalsIgnoreCase(connection.getConnectionFacility())
						|| WSCalculationConstant.MDMS_WATER_SEWERAGE_CONNECTION.equalsIgnoreCase(connection.getConnectionFacility()))) {
			List<Object> waterCessMasterList = timeBasedExemptionsMasterMap
					.get(WSCalculationConstant.WC_WATER_CESS_MASTER);
			BigDecimal waterCess;
			waterCess = waterCessUtil.getWaterCess(waterCharge, WSCalculationConstant.Assessment_Year, waterCessMasterList);
			if(waterCess.compareTo(BigDecimal.ZERO) > 0)	{
				estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_WATER_CESS)
						.estimateAmount(waterCess.setScale(2, 2)).build());
			}
		}
		
		// sewerage cess
		if (timeBasedExemptionsMasterMap.get(WSCalculationConstant.SW_SEWERAGE_CESS_MASTER) != null
				&& (WSCalculationConstant.MDMS_SEWERAGE_CONNECTION.equalsIgnoreCase(connection.getConnectionFacility())
						|| WSCalculationConstant.MDMS_WATER_SEWERAGE_CONNECTION.equalsIgnoreCase(connection.getConnectionFacility()))) {
			List<Object> sewerageCessMasterList = timeBasedExemptionsMasterMap
					.get(WSCalculationConstant.SW_SEWERAGE_CESS_MASTER);
			BigDecimal sewerageCess = waterCessUtil.getWaterCess(SewerageCharge,WSCalculationConstant.Assessment_Year, sewerageCessMasterList);
			if(sewerageCess.compareTo(BigDecimal.ZERO) > 0) {
				estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.SW_WATER_CESS)
						.estimateAmount(sewerageCess.setScale(2, 2)).build());
			}
		}
		
		//add water installment related taxheads
		addTaxHeadsForInstallments(connection, estimates);
		
		// Rebate Section
		applyRebate(waterCharge, SewerageCharge, criteria, timeBasedExemptionsMasterMap, requestInfoWrapper, estimates, masterData);
		
		return estimates;
	}
	
	private void applyRebate(BigDecimal waterCharge, BigDecimal SewerageCharge, CalculationCriteria criteria,
			Map<String, JSONArray> timeBasedExemptionsMasterMap, RequestInfoWrapper requestInfoWrapper,
			List<TaxHeadEstimate> estimates, Map<String, Object> masterData) {
		boolean isAnnualAdvanceRebateApplied = false;
		BigDecimal totalCharge = waterCharge.add(SewerageCharge);

		// Check if any advance available
		BillResponse billResponse = demandService.fetchBill(requestInfoWrapper.getRequestInfo(), criteria.getTenantId(), criteria.getConnectionNo());
		BigDecimal availableAdvance = BigDecimal.ZERO;
		if(billResponse != null) {
			availableAdvance = billResponse.getBill().get(0).getBillDetails().stream().flatMap(billDetails -> billDetails.getBillAccountDetails().stream())
				.filter(billAccDetail -> billAccDetail.getTaxHeadCode().contains("ADVANCE"))
				.map(BillAccountDetail::getAmount).reduce(BigDecimal.ZERO, BigDecimal::add);
		}

		if(availableAdvance.compareTo(BigDecimal.ZERO) < 0) {
			// Search for annual Advance
			BigDecimal annualPaymentRebate = BigDecimal.ZERO;
			List<AnnualAdvance> annualAdvances = annualAdvanceService.findAnnualPayment(criteria.getTenantId(), criteria.getConnectionNo(), calculatorUtil.detectAssessmentyear(configs.getDemandManualMonthNo(), configs.getDemandManualYear()));
			if(!annualAdvances.isEmpty()) {
				// if annual advance opted and have sufficient advance to knock off specified tax amounts then add annual advance rebate
				annualPaymentRebate = payService.getAnnualAdvanceRebate(waterCharge, SewerageCharge, calculatorUtil.detectAssessmentyear(configs.getDemandManualMonthNo(), configs.getDemandManualYear()), timeBasedExemptionsMasterMap.get(WSCalculationConstant.WC_ANNUAL_ADVANCE_MASTER));
				BigDecimal tillNowTotalDemand = estimates.stream().filter(the -> WSCalculationConstant.ANNUAL_ADVANCE_ALLOWED_TAXHEAD.contains(the.getTaxHeadCode()))
						.map(TaxHeadEstimate::getEstimateAmount).reduce(BigDecimal.ZERO, BigDecimal::add);
				// if have sufficient balance to knock-off the estimates 5% rebate for annual including rebate.
				BigDecimal payableIncludingAnnualRebate = tillNowTotalDemand.add(annualPaymentRebate.negate());
				if((availableAdvance.add(payableIncludingAnnualRebate)).compareTo(BigDecimal.ZERO) <= 0
						&& annualPaymentRebate.compareTo(BigDecimal.ZERO) > 0) {
					isAnnualAdvanceRebateApplied = true;
					estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_ANNUAL_PAYMENT_REBATE)
							.estimateAmount(annualPaymentRebate.setScale(2, 2).negate()).build());
				}
			} else {
				// For normal advance if have sufficient advance to knock off the entire demand then add time base rebate.
				BigDecimal timeBaseRebate = payService.getApplicableRebateForInitialDemand(totalCharge.setScale(2, 2), calculatorUtil.detectAssessmentyear(configs.getDemandManualMonthNo(), configs.getDemandManualYear()), timeBasedExemptionsMasterMap.get(WSCalculationConstant.WC_REBATE_MASTER));
				BigDecimal tillNowTotalDemand = estimates.stream().map(TaxHeadEstimate::getEstimateAmount).reduce(BigDecimal.ZERO, BigDecimal::add);
				BigDecimal payableIncludingRebate = tillNowTotalDemand.add(timeBaseRebate.negate());
				if((availableAdvance.add(payableIncludingRebate)).compareTo(BigDecimal.ZERO) <= 0
						&& timeBaseRebate.compareTo(BigDecimal.ZERO) > 0) {
					estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_TIME_REBATE)
							.estimateAmount(timeBaseRebate.setScale(2, 2).negate()).build());
				}
			}
		}

		// water Special Rebate if annual advance rebate not applied
		if(isSpecialRebateApplicableForMonth(criteria, masterData) && !isAnnualAdvanceRebateApplied) {
			BigDecimal specialRebate = payService.getApplicableSpecialRebate(totalCharge.setScale(2, 2), calculatorUtil.getAssessmentYear(), timeBasedExemptionsMasterMap.get(WSCalculationConstant.WC_REBATE_MASTER));
			if(specialRebate.compareTo(BigDecimal.ZERO) > 0) {
				estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_SPECIAL_REBATE)
						.estimateAmount(specialRebate.setScale(2, 2).negate()).build());
			}
		}
	}

	/**
	 * method to do a first level filtering on the slabs based on the values
	 * present in the Water Details
	 */

	public BigDecimal getWaterEstimationCharge(WaterConnection waterConnection, CalculationCriteria criteria, 
			Map<String, JSONArray> billingSlabMaster, ArrayList<String> billingSlabIds, RequestInfo requestInfo) {
		BigDecimal waterCharge = BigDecimal.ZERO;
		if (billingSlabMaster.get(WSCalculationConstant.WC_BILLING_SLAB_MASTER) == null)
			throw new CustomException("BILLING_SLAB_NOT_FOUND", "Billing Slab are Empty");
		List<BillingSlab> mappingBillingSlab;
		try {
			mappingBillingSlab = mapper.readValue(
					billingSlabMaster.get(WSCalculationConstant.WC_BILLING_SLAB_MASTER).toJSONString(),
					mapper.getTypeFactory().constructCollectionType(List.class, BillingSlab.class));
		} catch (IOException e) {
			throw new CustomException("PARSING_ERROR", "Billing Slab can not be parsed!");
		}
		JSONObject calculationAttributeMaster = new JSONObject();
		calculationAttributeMaster.put(WSCalculationConstant.CALCULATION_ATTRIBUTE_CONST, billingSlabMaster.get(WSCalculationConstant.CALCULATION_ATTRIBUTE_CONST));
        String calculationAttribute = getCalculationAttribute(calculationAttributeMaster, waterConnection.getConnectionType());
		List<BillingSlab> billingSlabs = getSlabsFiltered(waterConnection, mappingBillingSlab, calculationAttribute, requestInfo);
		if (billingSlabs == null || billingSlabs.isEmpty())
			throw new CustomException("BILLING_SLAB_NOT_FOUND", "Billing Slab are Empty");
		if (billingSlabs.size() > 1)
			throw new CustomException("INVALID_BILLING_SLAB",
					"More than one billing slab found");
		billingSlabIds.add(billingSlabs.get(0).getId());
		log.debug(" Billing Slab Id For Water Charge Calculation --->  " + billingSlabIds.toString());

		// WaterCharge Calculation
		Double totalUOM = getUnitOfMeasurement(waterConnection, calculationAttribute, criteria);
		if (totalUOM == 0.0)
			return waterCharge;
		BillingSlab billSlab = billingSlabs.get(0);
		// IF calculation type is flat then take flat rate else take slab and calculate the charge
		//For metered connection calculation on graded fee slab
		//For Non metered connection calculation on normal connection
		if (isRangeCalculation(calculationAttribute)) {
			if (waterConnection.getConnectionType().equalsIgnoreCase(WSCalculationConstant.meteredConnectionType)) {
				for (Slab slab : billSlab.getSlabs()) {
					if (totalUOM > slab.getTo()) {
						waterCharge = waterCharge.add(BigDecimal.valueOf(((slab.getTo()) - (slab.getFrom())) * slab.getCharge()));
						totalUOM = totalUOM - ((slab.getTo()) - (slab.getFrom()));
					} else if (totalUOM < slab.getTo()) {
						waterCharge = waterCharge.add(BigDecimal.valueOf(totalUOM * slab.getCharge()));
						totalUOM = ((slab.getTo()) - (slab.getFrom())) - totalUOM;
						break;
					}
				}
				if (billSlab.getMinimumCharge() > waterCharge.doubleValue()) {
					waterCharge = BigDecimal.valueOf(billSlab.getMinimumCharge());
				}
			} else if (waterConnection.getConnectionType()
					.equalsIgnoreCase(WSCalculationConstant.nonMeterdConnection)) {
				for (Slab slab : billSlab.getSlabs()) {
					if (totalUOM >= slab.getFrom() && totalUOM < slab.getTo()) {
						waterCharge = BigDecimal.valueOf((totalUOM * slab.getCharge()));
						if (billSlab.getMinimumCharge() > waterCharge.doubleValue()) {
							waterCharge = BigDecimal.valueOf(billSlab.getMinimumCharge());
						}
						break;
					}
				}
			}
		} else {
			waterCharge = BigDecimal.valueOf(billSlab.getMinimumCharge());
		}
		return waterCharge;
	}

	private List<BillingSlab> getSlabsFiltered(WaterConnection waterConnection, List<BillingSlab> billingSlabs,
			String calculationAttribute, RequestInfo requestInfo) {

		// Property property = wSCalculationUtil.getProperty(
		// 		WaterConnectionRequest.builder().waterConnection(waterConnection).requestInfo(requestInfo).build());
		// get billing Slab
		log.debug(" the slabs count : " + billingSlabs.size());
		final String buildingType = (waterConnection.getUsageCategory() != null) ? waterConnection.getUsageCategory().split("\\.")[0]
				: "";
		// final String buildingType = "Domestic";
		final String connectionType = waterConnection.getConnectionType();

		return billingSlabs.stream().filter(slab -> {
			boolean isBuildingTypeMatching = slab.getBuildingType().equalsIgnoreCase(buildingType);
			boolean isConnectionTypeMatching = slab.getConnectionType().equalsIgnoreCase(connectionType);
			boolean isCalculationAttributeMatching = slab.getCalculationAttribute()
					.equalsIgnoreCase(calculationAttribute);
			return isBuildingTypeMatching && isConnectionTypeMatching && isCalculationAttributeMatching;
		}).collect(Collectors.toList());
	}
	
	private String getCalculationAttribute(Map<String, Object> calculationAttributeMap, String connectionType) {
		if (calculationAttributeMap == null)
			throw new CustomException("CALCULATION_ATTRIBUTE_MASTER_NOT_FOUND",
					"Calculation attribute master not found!!");
		JSONArray filteredMasters = JsonPath.read(calculationAttributeMap,
				"$.CalculationAttribute[?(@.name=='" + connectionType + "')]");
		JSONObject master = mapper.convertValue(filteredMasters.get(0), JSONObject.class);
		return master.getAsString(WSCalculationConstant.ATTRIBUTE);
	}
	
	/**
	 * 
	 * @param type will be calculation Attribute
	 * @return true if calculation Attribute is not Flat else false
	 */
	private boolean isRangeCalculation(String type) {
		return !type.equalsIgnoreCase(WSCalculationConstant.flatRateCalculationAttribute);
	}
	
	private Double getUnitOfMeasurement(WaterConnection waterConnection, String calculationAttribute,
			CalculationCriteria criteria) {
		Double totalUnit = 0.0;
		if (waterConnection.getConnectionType().equals(WSCalculationConstant.meteredConnectionType)) {
			totalUnit = (criteria.getCurrentReading() - criteria.getLastReading());
			return totalUnit;
		} else if (waterConnection.getConnectionType().equals(WSCalculationConstant.nonMeterdConnection)
				&& calculationAttribute.equalsIgnoreCase(WSCalculationConstant.noOfTapsConst)) {
			if (waterConnection.getNoOfTaps() == null)
				return totalUnit;
			return new Double(waterConnection.getNoOfTaps());
		} else if (waterConnection.getConnectionType().equals(WSCalculationConstant.nonMeterdConnection)
				&& calculationAttribute.equalsIgnoreCase(WSCalculationConstant.pipeSizeConst)) {
			if (waterConnection.getPipeSize() == null)
				return totalUnit;
			return waterConnection.getPipeSize();
		}
		return 0.0;
	}
	
	public Map<String, Object> getQuarterStartAndEndDate(Map<String, Object> billingPeriod){
		Date date = new Date();
		Calendar fromDateCalendar = Calendar.getInstance();
		fromDateCalendar.setTime(date);
		fromDateCalendar.set(Calendar.MONTH, fromDateCalendar.get(Calendar.MONTH)/3 * 3);
		fromDateCalendar.set(Calendar.DAY_OF_MONTH, 1);
		setTimeToBeginningOfDay(fromDateCalendar);
		Calendar toDateCalendar = Calendar.getInstance();
		toDateCalendar.setTime(date);
		toDateCalendar.set(Calendar.MONTH, toDateCalendar.get(Calendar.MONTH)/3 * 3 + 2);
		toDateCalendar.set(Calendar.DAY_OF_MONTH, toDateCalendar.getActualMaximum(Calendar.DAY_OF_MONTH));
		setTimeToEndofDay(toDateCalendar);
		billingPeriod.put(WSCalculationConstant.STARTING_DATE_APPLICABLES, fromDateCalendar.getTimeInMillis());
		billingPeriod.put(WSCalculationConstant.ENDING_DATE_APPLICABLES, toDateCalendar.getTimeInMillis());
		return billingPeriod;
	}
	
	public Map<String, Object> getMonthStartAndEndDate(Map<String, Object> billingPeriod){
		Date date = new Date();
		Calendar monthStartDate = Calendar.getInstance();
		monthStartDate.setTime(date);
		if (configs.isDemandStartEndDateManuallyConfigurable()) {
			monthStartDate.set(Calendar.MONTH, configs.getDemandManualMonthNo() - 1);
			monthStartDate.set(Calendar.YEAR, configs.getDemandManualYear());
		}
		monthStartDate.set(Calendar.DAY_OF_MONTH, monthStartDate.getActualMinimum(Calendar.DAY_OF_MONTH));
		setTimeToBeginningOfDay(monthStartDate);
	    
		Calendar monthEndDate = Calendar.getInstance();
		monthEndDate.setTime(date);
		if (configs.isDemandStartEndDateManuallyConfigurable()) {
			monthEndDate.set(Calendar.MONTH, configs.getDemandManualMonthNo() - 1);
			monthEndDate.set(Calendar.YEAR, configs.getDemandManualYear());
		}
		monthEndDate.set(Calendar.DAY_OF_MONTH, monthEndDate.getActualMaximum(Calendar.DAY_OF_MONTH));
		setTimeToEndofDay(monthEndDate);
		billingPeriod.put(WSCalculationConstant.STARTING_DATE_APPLICABLES, monthStartDate.getTimeInMillis());
		billingPeriod.put(WSCalculationConstant.ENDING_DATE_APPLICABLES, monthEndDate.getTimeInMillis());
		return billingPeriod;
	}
	
	private static void setTimeToBeginningOfDay(Calendar calendar) {
	    calendar.set(Calendar.HOUR_OF_DAY, 0);
	    calendar.set(Calendar.MINUTE, 0);
	    calendar.set(Calendar.SECOND, 0);
	    calendar.set(Calendar.MILLISECOND, 0);
	}

	private static void setTimeToEndofDay(Calendar calendar) {
	    calendar.set(Calendar.HOUR_OF_DAY, 18);
	    calendar.set(Calendar.MINUTE, 29);
	    calendar.set(Calendar.SECOND, 0);
	    calendar.set(Calendar.MILLISECOND, 0);
	}
	
	
	/**
	 * 
	 * @param criteria - Calculation Search Criteria
	 * @param requestInfo - Request Info Object
	 * @param masterData - Master Data map
	 * @return Fee Estimation Map
	 */
	@SuppressWarnings("rawtypes")
	public Map<String, List> getFeeEstimation(CalculationCriteria criteria, RequestInfo requestInfo,
			Map<String, Object> masterData, boolean isEstimate) {
		if (StringUtils.isEmpty(criteria.getWaterConnection()) && !StringUtils.isEmpty(criteria.getApplicationNo())) {
			SearchCriteria searchCriteria = new SearchCriteria();
			searchCriteria.setApplicationNumber(criteria.getApplicationNo());
			searchCriteria.setTenantId(criteria.getTenantId());
			WaterConnection waterConnection = calculatorUtil.getWaterConnectionOnApplicationNO(requestInfo, searchCriteria, requestInfo.getUserInfo().getTenantId());
			criteria.setWaterConnection(waterConnection);
		}
		if (StringUtils.isEmpty(criteria.getWaterConnection())) {
			throw new CustomException("WATER_CONNECTION_NOT_FOUND",
					"Water Connection are not present for " + criteria.getApplicationNo() + " Application no");
		}
		ArrayList<String> billingSlabIds = new ArrayList<>();
		billingSlabIds.add("");
		// List<TaxHeadEstimate> taxHeadEstimates = getTaxHeadForFeeEstimation(criteria, masterData, requestInfo);
		List<TaxHeadEstimate> taxHeadEstimates = getTaxHeadForFeeEstimationV2(criteria, requestInfo, masterData, isEstimate);
		Map<String, List> estimatesAndBillingSlabs = new HashMap<>();
		estimatesAndBillingSlabs.put("estimates", taxHeadEstimates);
		// //Billing slab id
		estimatesAndBillingSlabs.put("billingSlabIds", billingSlabIds);
		return estimatesAndBillingSlabs;
	}

	private List<TaxHeadEstimate> getTaxHeadForFeeEstimationV2(CalculationCriteria criteria, RequestInfo requestInfo, Map<String, Object> masterData, boolean isEstimate) {
		List<TaxHeadEstimate> taxHeadEstimate = new ArrayList<>();
		boolean isWater = false;
		boolean isSewerage = false;
		if(WSCalculationConstant.MDMS_WATER_CONNECTION.equalsIgnoreCase(criteria.getWaterConnection().getConnectionFacility())
				|| WSCalculationConstant.MDMS_WATER_SEWERAGE_CONNECTION.equalsIgnoreCase(criteria.getWaterConnection().getConnectionFacility())) {
			isWater = true;
		}
		if(WSCalculationConstant.MDMS_SEWERAGE_CONNECTION.equalsIgnoreCase(criteria.getWaterConnection().getConnectionFacility())
				|| WSCalculationConstant.MDMS_WATER_SEWERAGE_CONNECTION.equalsIgnoreCase(criteria.getWaterConnection().getConnectionFacility())) {
			isSewerage = true;
		}
		
		if(isWater) {
			taxHeadEstimate.addAll(getWaterTaxHeadForFeeEstimationV2(criteria, requestInfo, masterData, isEstimate));
		}
		if(isSewerage) {
			taxHeadEstimate.addAll(getSewerageTaxHeadForFeeEstimationV2(criteria, requestInfo, masterData));
		}
		
		return taxHeadEstimate;
	}

	private List<TaxHeadEstimate> getSewerageTaxHeadForFeeEstimationV2(CalculationCriteria criteria,
			RequestInfo requestInfo, Map<String, Object> masterData) {

		BigDecimal scrutinyFee = BigDecimal.ZERO;
		BigDecimal securityCharge = BigDecimal.ZERO;
		if (criteria.getWaterConnection().getConnectionCategory().equalsIgnoreCase("Permanent")) {
			securityCharge = BigDecimal.ZERO;
			switch (criteria.getWaterConnection().getUsageCategory().toUpperCase()) {
				case "COMMERCIAL":
					securityCharge = BigDecimal.valueOf(60);
					if (criteria.getWaterConnection().getNoOfFlats() > 0
							&& criteria.getWaterConnection().getNoOfFlats() <= 25) {
						scrutinyFee = BigDecimal.valueOf(5000);
						securityCharge = BigDecimal.valueOf(60);
					} else if (criteria.getWaterConnection().getNoOfFlats() > 25
							&& criteria.getWaterConnection().getNoOfFlats() <= 50) {
						scrutinyFee = BigDecimal.valueOf(10000);
						securityCharge = BigDecimal.valueOf(60);
					} else if (criteria.getWaterConnection().getNoOfFlats() > 50) {
						scrutinyFee = BigDecimal.valueOf(15000);
						securityCharge = BigDecimal.valueOf(60);
					} else {
						scrutinyFee = BigDecimal.valueOf(3500);
					}
					break;
				case "INDUSTRIAL":
					scrutinyFee = BigDecimal.valueOf(3500);
					securityCharge = BigDecimal.valueOf(60);
					break;
				case "INSTITUTIONAL":
					scrutinyFee = BigDecimal.valueOf(2500);
					securityCharge = BigDecimal.valueOf(60);
					break;
				case "DOMESTIC":
					if (criteria.getWaterConnection().getNoOfFlats() > 0
							&& criteria.getWaterConnection().getNoOfFlats() <= 25) {
						scrutinyFee = BigDecimal.valueOf(5000);
						securityCharge = BigDecimal.valueOf(60);
					} else if (criteria.getWaterConnection().getNoOfFlats() > 25
							&& criteria.getWaterConnection().getNoOfFlats() <= 50) {
						scrutinyFee = BigDecimal.valueOf(10000);
						securityCharge = BigDecimal.valueOf(60);
					} else if (criteria.getWaterConnection().getNoOfFlats() > 50) {
						scrutinyFee = BigDecimal.valueOf(15000);
						securityCharge = BigDecimal.valueOf(60);
					}
					break;
			}
		}

		List<TaxHeadEstimate> estimates = new ArrayList<>();
		estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.SW_SCRUTINY_FEE)
				.estimateAmount(scrutinyFee.setScale(2, 2)).build());
		// Security charge is not applicable now
		// TODO: Need to remove the security fee component
//		estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.SW_SECURITY_CHARGE)
//				.estimateAmount(securityCharge.setScale(2, 2)).build());

		return estimates;
	}

	private List<TaxHeadEstimate> getWaterTaxHeadForFeeEstimationV2(CalculationCriteria criteria, RequestInfo requestInfo, Map<String, Object> masterData, boolean isEstimate) {

		// Property property = wSCalculationUtil.getProperty(WaterConnectionRequest.builder()
				// 		.waterConnection(criteria.getWaterConnection()).requestInfo(requestInfo).build());
				
				String usageCategory = criteria.getWaterConnection().getUsageCategory();
				BigDecimal scrutinyFee = BigDecimal.ZERO;
				BigDecimal securityCharge  = BigDecimal.ZERO;
				if(criteria.getWaterConnection().getConnectionType().equalsIgnoreCase(WSCalculationConstant.meteredConnectionType)) {
					if(criteria.getWaterConnection().getConnectionCategory().equalsIgnoreCase("Permanent")) {
						securityCharge = BigDecimal.valueOf(60);
						switch(criteria.getWaterConnection().getUsageCategory().toUpperCase()) {
						case WSCalculationConstant.WS_UC_COMMERCIAL:
							if(criteria.getWaterConnection().getNoOfFlats() > 0 && criteria.getWaterConnection().getNoOfFlats() <= 25) {
								scrutinyFee = BigDecimal.valueOf(10000);
							} else if(criteria.getWaterConnection().getNoOfFlats() > 25 && criteria.getWaterConnection().getNoOfFlats() <= 50) {
								scrutinyFee = BigDecimal.valueOf(20000);
							} else if(criteria.getWaterConnection().getNoOfFlats() > 50) {
								scrutinyFee = BigDecimal.valueOf(30000);
							} else {
								scrutinyFee = BigDecimal.valueOf(6000);
							}
							break;
						case WSCalculationConstant.WS_UC_INDUSTRIAL:
							scrutinyFee = BigDecimal.valueOf(6000);
							break;
						case WSCalculationConstant.WS_UC_INSTITUTIONAL:
							scrutinyFee = BigDecimal.valueOf(5000);
							break;
						case WSCalculationConstant.WS_UC_DOMESTIC:
							if(criteria.getWaterConnection().getNoOfFlats() > 0 && criteria.getWaterConnection().getNoOfFlats() <= 25) {
								scrutinyFee = BigDecimal.valueOf(10000);
							} else if(criteria.getWaterConnection().getNoOfFlats() > 25 && criteria.getWaterConnection().getNoOfFlats() <= 50) {
								scrutinyFee = BigDecimal.valueOf(20000);
							} else if(criteria.getWaterConnection().getNoOfFlats() > 50) {
								scrutinyFee = BigDecimal.valueOf(30000);
							} else {
								scrutinyFee = BigDecimal.valueOf(3000);
							}
							break;
						case WSCalculationConstant.WS_UC_ASSOCIATION:
							scrutinyFee = BigDecimal.valueOf(6000);
							break;
						case WSCalculationConstant.WS_UC_BPL:
							securityCharge = BigDecimal.ZERO;
							break;
						}
					} else if(criteria.getWaterConnection().getConnectionCategory().equalsIgnoreCase("Temporary")) {
						if(criteria.getWaterConnection().getNoOfFlats() > 0 && criteria.getWaterConnection().getNoOfFlats() <= 25) {
							scrutinyFee = BigDecimal.valueOf(10000);
						} else if(criteria.getWaterConnection().getNoOfFlats() > 25 && criteria.getWaterConnection().getNoOfFlats() <= 50) {
							scrutinyFee = BigDecimal.valueOf(20000);
						} else if(criteria.getWaterConnection().getNoOfFlats() > 50) {
							scrutinyFee = BigDecimal.valueOf(30000);
						} else if(WSCalculationConstant.WS_UC_TWSFC.equalsIgnoreCase(usageCategory)) {
							scrutinyFee = BigDecimal.valueOf(500);
							securityCharge = BigDecimal.ZERO;
						} else {
							scrutinyFee = BigDecimal.valueOf(3000);
						}
					}
				} else if(criteria.getWaterConnection().getConnectionType().equalsIgnoreCase(WSCalculationConstant.nonMeterdConnection)) {
					if(criteria.getWaterConnection().getConnectionCategory().equalsIgnoreCase("Permanent")) {
						securityCharge = BigDecimal.ZERO;
						switch(criteria.getWaterConnection().getUsageCategory().toUpperCase()) {
						case WSCalculationConstant.WS_UC_DOMESTIC:
							scrutinyFee = BigDecimal.valueOf(3000);
							securityCharge = BigDecimal.valueOf(60);
							break;
						}
					} else if(criteria.getWaterConnection().getConnectionCategory().equalsIgnoreCase("Temporary")) {
						if(WSCalculationConstant.WS_UC_DOMESTIC.equals(criteria.getWaterConnection().getUsageCategory().toUpperCase())) {
							scrutinyFee = BigDecimal.valueOf(3000);
							securityCharge = BigDecimal.valueOf(60);
						} else if(WSCalculationConstant.WS_UC_ROADSIDEEATERS.equals(criteria.getWaterConnection().getUsageCategory().toUpperCase())) {
							scrutinyFee = BigDecimal.valueOf(500);
							securityCharge = BigDecimal.valueOf(60);
						}
					}
				}

				BigDecimal labourFee = calculateLabourFee(requestInfo, criteria.getWaterConnection(), masterData, isEstimate);
				//Scrutiny Fee Installments only applicable for non-metered and domestic connections
				if(WSCalculationConstant.CONNECTION_PERMANENT.equalsIgnoreCase(criteria.getWaterConnection().getConnectionCategory())
						&& WSCalculationConstant.WS_UC_DOMESTIC.equalsIgnoreCase(criteria.getWaterConnection().getUsageCategory())
						&& criteria.getWaterConnection().getNoOfFlats().compareTo(0) <= 0) {

					log.info("Applying installments for scrutiny fee for connection : "+ criteria.getWaterConnection().getConnectionNo());
					scrutinyFee = applyScrutinyFeeInstallments(criteria.getWaterConnection(), requestInfo, scrutinyFee, isEstimate);
				}
				
				List<TaxHeadEstimate> estimates = new ArrayList<>();
				estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_SCRUTINY_FEE)
						.estimateAmount(scrutinyFee.setScale(2, 2)).build());
				// Security charge is not applicable now
				// TODO: Need to remove the security fee component
//				estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_SECURITY_CHARGE)
//						.estimateAmount(securityCharge.setScale(2, 2)).build());
				
				if(labourFee.compareTo(BigDecimal.ZERO) > 0) {
					estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_LABOUR_FEE)
							.estimateAmount(labourFee.setScale(2, 2)).build());
				}
				
				return estimates;
	
	}
	private BigDecimal calculateLabourFee(RequestInfo requestInfo, WaterConnection waterConnection, Map<String, Object> masterData, boolean isEstimate) {
		BigDecimal labourFee = BigDecimal.ZERO;
		boolean isLabourFeeApplicable = Boolean.FALSE;
		boolean isInstallmentApplicable = Boolean.FALSE;
		if(waterConnection.getAdditionalDetails() == null) {
			return labourFee;
		}
		LinkedHashMap additionalDetailJsonNode = (LinkedHashMap)waterConnection.getAdditionalDetails();
		if(additionalDetailJsonNode.containsKey(WSCalculationConstant.IS_LABOUR_FEE_APPLICABLE)
				&& !StringUtils.isEmpty(additionalDetailJsonNode.get(WSCalculationConstant.IS_LABOUR_FEE_APPLICABLE))) {
			isLabourFeeApplicable = additionalDetailJsonNode.get(WSCalculationConstant.IS_LABOUR_FEE_APPLICABLE).toString().equalsIgnoreCase("Y") 
					? Boolean.TRUE:Boolean.FALSE;
		}
		
		if(additionalDetailJsonNode.containsKey(WSCalculationConstant.IS_INSTALLMENT_APPLICABLE)
				&& !StringUtils.isEmpty(additionalDetailJsonNode.get(WSCalculationConstant.IS_INSTALLMENT_APPLICABLE))) {
			isInstallmentApplicable = additionalDetailJsonNode.get(WSCalculationConstant.IS_INSTALLMENT_APPLICABLE).toString().equalsIgnoreCase("Y") 
					? Boolean.TRUE:Boolean.FALSE;
		}
		
		JSONArray labourFeeMaster = (JSONArray) masterData.getOrDefault(WSCalculationConstant.WC_LABOURFEE_MASTER, null);
		if (labourFeeMaster == null)
			throw new CustomException("LABOUR_FEE_NOT_FOUND", "labour fee master data not found!!");
		
		JSONObject labourFeeObj = mapper.convertValue(labourFeeMaster.get(0), JSONObject.class);
		
		if(isLabourFeeApplicable && isInstallmentApplicable) {
			if((WSCalculationConstant.meteredConnectionType.equalsIgnoreCase(waterConnection.getConnectionType())
					&& ((WSCalculationConstant.WS_UC_DOMESTIC.equalsIgnoreCase(waterConnection.getUsageCategory())
							&& waterConnection.getNoOfFlats().compareTo(0) <= 0)
				|| WSCalculationConstant.WS_UC_BPL.equalsIgnoreCase(waterConnection.getUsageCategory())))
			|| (WSCalculationConstant.nonMeterdConnection.equalsIgnoreCase(waterConnection.getConnectionType())
					&& ((WSCalculationConstant.CONNECTION_TEMPORARY.equalsIgnoreCase(waterConnection.getConnectionCategory())
							&& WSCalculationConstant.WS_UC_ROADSIDEEATERS.equalsIgnoreCase(waterConnection.getUsageCategory()))
						|| (WSCalculationConstant.CONNECTION_PERMANENT.equalsIgnoreCase(waterConnection.getConnectionCategory())
								&& ((WSCalculationConstant.WS_UC_DOMESTIC.equalsIgnoreCase(waterConnection.getUsageCategory())
										&& waterConnection.getNoOfFlats().compareTo(0) <= 0)
									|| WSCalculationConstant.WS_UC_BPL.equalsIgnoreCase(waterConnection.getUsageCategory())))))) {
				labourFee = applyLabourFeeInstallments(waterConnection, requestInfo, isEstimate);
			}
			
		} else if(isLabourFeeApplicable && !isInstallmentApplicable) {
			if((WSCalculationConstant.meteredConnectionType.equalsIgnoreCase(waterConnection.getConnectionType())
					&& ((WSCalculationConstant.WS_UC_DOMESTIC.equalsIgnoreCase(waterConnection.getUsageCategory())
							&& waterConnection.getNoOfFlats().compareTo(0) <= 0)
				|| WSCalculationConstant.WS_UC_BPL.equalsIgnoreCase(waterConnection.getUsageCategory())))
			|| (WSCalculationConstant.nonMeterdConnection.equalsIgnoreCase(waterConnection.getConnectionType())
					&& ((WSCalculationConstant.CONNECTION_TEMPORARY.equalsIgnoreCase(waterConnection.getConnectionCategory())
							&& WSCalculationConstant.WS_UC_ROADSIDEEATERS.equalsIgnoreCase(waterConnection.getUsageCategory()))
						|| (WSCalculationConstant.CONNECTION_PERMANENT.equalsIgnoreCase(waterConnection.getConnectionCategory())
								&& ((WSCalculationConstant.WS_UC_DOMESTIC.equalsIgnoreCase(waterConnection.getUsageCategory())
										&& waterConnection.getNoOfFlats().compareTo(0) <= 0)
									|| WSCalculationConstant.WS_UC_BPL.equalsIgnoreCase(waterConnection.getUsageCategory())))))) {
				
				if (labourFeeObj.get(WSCalculationConstant.LABOURFEE_TOTALAMOUNT) != null) {
					labourFee = new BigDecimal(labourFeeObj.getAsNumber(WSCalculationConstant.LABOURFEE_TOTALAMOUNT).toString());
				}
			}
		}
		
		return labourFee;
	}
	
	/**
	 * 
	 * @param criteria Calculation Search Criteria
	 * @param masterData - Master Data
	 * @param requestInfo - RequestInfo
	 * @return return all tax heads
	 */
	private List<TaxHeadEstimate> getTaxHeadForFeeEstimation(CalculationCriteria criteria,
			Map<String, Object> masterData, RequestInfo requestInfo) {
		JSONArray feeSlab = (JSONArray) masterData.getOrDefault(WSCalculationConstant.WC_FEESLAB_MASTER, null);
		if (feeSlab == null)
			throw new CustomException("FEE_SLAB_NOT_FOUND", "fee slab master data not found!!");

			WaterConnectionRequest waterConnectionRequest = WaterConnectionRequest.builder()
					.waterConnection(criteria.getWaterConnection()).requestInfo(requestInfo).build();

		// Property property = wSCalculationUtil.getProperty(waterConnectionRequest);
		
		JSONObject feeObj = mapper.convertValue(feeSlab.get(0), JSONObject.class);
		BigDecimal formFee = BigDecimal.ZERO;
		if (feeObj.get(WSCalculationConstant.FORM_FEE_CONST) != null) {
			formFee = new BigDecimal(feeObj.getAsNumber(WSCalculationConstant.FORM_FEE_CONST).toString());
		}
		BigDecimal scrutinyFee = BigDecimal.ZERO;
		if (feeObj.get(WSCalculationConstant.SCRUTINY_FEE_CONST) != null) {
			scrutinyFee = new BigDecimal(feeObj.getAsNumber(WSCalculationConstant.SCRUTINY_FEE_CONST).toString());
		}
		BigDecimal otherCharges = BigDecimal.ZERO;
		if (feeObj.get(WSCalculationConstant.OTHER_CHARGE_CONST) != null) {
			otherCharges = new BigDecimal(feeObj.getAsNumber(WSCalculationConstant.OTHER_CHARGE_CONST).toString());
		}
		BigDecimal taxAndCessPercentage = BigDecimal.ZERO;
		if (feeObj.get(WSCalculationConstant.TAX_PERCENTAGE_CONST) != null) {
			taxAndCessPercentage = new BigDecimal(
					feeObj.getAsNumber(WSCalculationConstant.TAX_PERCENTAGE_CONST).toString());
		}
		BigDecimal meterCost = BigDecimal.ZERO;
		if (feeObj.get(WSCalculationConstant.METER_COST_CONST) != null
				&& criteria.getWaterConnection().getConnectionType() != null && criteria.getWaterConnection()
						.getConnectionType().equalsIgnoreCase(WSCalculationConstant.meteredConnectionType)) {
			meterCost = new BigDecimal(feeObj.getAsNumber(WSCalculationConstant.METER_COST_CONST).toString());
		}
		BigDecimal roadCuttingCharge = BigDecimal.ZERO;
		BigDecimal usageTypeCharge = BigDecimal.ZERO;

		if(criteria.getWaterConnection().getRoadCuttingInfo() != null){
			for(RoadCuttingInfo roadCuttingInfo : criteria.getWaterConnection().getRoadCuttingInfo()){
				BigDecimal singleRoadCuttingCharge = BigDecimal.ZERO;
				if (roadCuttingInfo.getRoadType() != null)
					singleRoadCuttingCharge = getChargeForRoadCutting(masterData, roadCuttingInfo.getRoadType(),
							roadCuttingInfo.getRoadCuttingArea());
							roadCuttingCharge = roadCuttingCharge.add(singleRoadCuttingCharge);
						}
					}

				BigDecimal singleUsageTypeCharge = BigDecimal.ZERO;
				if(criteria.getWaterConnection().getUsageCategory() != null){ 
				// if (roadCuttingInfo.getRoadCuttingArea() != null)
					singleUsageTypeCharge = getUsageTypeFee(masterData,
							waterConnectionRequest.getWaterConnection().getUsageCategory(),
							waterConnectionRequest.getWaterConnection().getConnectionCategory(),
							waterConnectionRequest.getWaterConnection().getConnectionType(),
							waterConnectionRequest.getWaterConnection().getNoOfFlats());
							// roadCuttingInfo.getRoadCuttingArea()
				}
				
				usageTypeCharge = usageTypeCharge.add(singleUsageTypeCharge);
			

		/**
		 * As landArea charges are not necessary for water connection the below code is commented
		 */

		// BigDecimal roadPlotCharge = BigDecimal.ZERO;
		// if (property.getLandArea() != null)
		// 	roadPlotCharge = getPlotSizeFee(masterData, property.getLandArea());

		BigDecimal totalCharge = formFee.add(scrutinyFee).add(otherCharges).add(meterCost).add(roadCuttingCharge)
				.add(usageTypeCharge); //.add(roadPlotCharge)
		BigDecimal tax = totalCharge.multiply(taxAndCessPercentage.divide(WSCalculationConstant.HUNDRED));
		List<TaxHeadEstimate> estimates = new ArrayList<>();
		//
		if (!(formFee.compareTo(BigDecimal.ZERO) == 0))
			estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_FORM_FEE)
					.estimateAmount(formFee.setScale(2, 2)).build());
		if (!(scrutinyFee.compareTo(BigDecimal.ZERO) == 0))
			estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_SCRUTINY_FEE)
					.estimateAmount(scrutinyFee.setScale(2, 2)).build());
		if (!(meterCost.compareTo(BigDecimal.ZERO) == 0))
			estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_METER_CHARGE)
					.estimateAmount(meterCost.setScale(2, 2)).build());
		if (!(otherCharges.compareTo(BigDecimal.ZERO) == 0))
			estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_OTHER_CHARGE)
					.estimateAmount(otherCharges.setScale(2, 2)).build());
		if (!(roadCuttingCharge.compareTo(BigDecimal.ZERO) == 0))
			estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_ROAD_CUTTING_CHARGE)
					.estimateAmount(roadCuttingCharge.setScale(2, 2)).build());
		if (!(usageTypeCharge.compareTo(BigDecimal.ZERO) == 0))
			estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_ONE_TIME_FEE)
					.estimateAmount(usageTypeCharge.setScale(2, 2)).build());
		// if (!(roadPlotCharge.compareTo(BigDecimal.ZERO) == 0))
		// 	estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_SECURITY_CHARGE)
		// 			.estimateAmount(roadPlotCharge.setScale(2, 2)).build());
		if (!(tax.compareTo(BigDecimal.ZERO) == 0))
			estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_TAX_AND_CESS)
					.estimateAmount(tax.setScale(2, 2)).build());
		addAdhocPenaltyAndRebate(estimates, criteria.getWaterConnection());
		return estimates;
	}
	
	/**
	 * 
	 * @param masterData Master Data Map
	 * @param roadType - Road type
	 * @param roadCuttingArea - Road Cutting Area
	 * @return road cutting charge
	 */
	private BigDecimal getChargeForRoadCutting(Map<String, Object> masterData, String roadType, Float roadCuttingArea) {
		JSONArray roadSlab = (JSONArray) masterData.getOrDefault(WSCalculationConstant.WC_ROADTYPE_MASTER, null);
		BigDecimal charge = BigDecimal.ZERO;
		JSONObject masterSlab = new JSONObject();
		if(roadSlab != null) {
			masterSlab.put("RoadType", roadSlab);
			JSONArray filteredMasters = JsonPath.read(masterSlab, "$.RoadType[?(@.code=='" + roadType + "')]");
			if (CollectionUtils.isEmpty(filteredMasters))
				return BigDecimal.ZERO;
			JSONObject master = mapper.convertValue(filteredMasters.get(0), JSONObject.class);
			charge = new BigDecimal(master.getAsNumber(WSCalculationConstant.UNIT_COST_CONST).toString());
			charge = charge.multiply(
					new BigDecimal(roadCuttingArea == null ? BigDecimal.ZERO.toString() : roadCuttingArea.toString()));
		}
		return charge;
	}
	
	/**
	 * 
	 * @param masterData - Master Data Map
	 * @param plotSize - Plot Size
	 * @return get fee based on plot size
	 */
	private BigDecimal getPlotSizeFee(Map<String, Object> masterData, Double plotSize) {
		BigDecimal charge = BigDecimal.ZERO;
		JSONArray plotSlab = (JSONArray) masterData.getOrDefault(WSCalculationConstant.WC_PLOTSLAB_MASTER, null);
		JSONObject masterSlab = new JSONObject();
		if (plotSlab != null) {
			masterSlab.put("PlotSizeSlab", plotSlab);
			JSONArray filteredMasters = JsonPath.read(masterSlab, "$.PlotSizeSlab[?(@.from <="+ plotSize +"&& @.to > " + plotSize +")]");
			if(CollectionUtils.isEmpty(filteredMasters))
				return charge;
			JSONObject master = mapper.convertValue(filteredMasters.get(0), JSONObject.class);
			charge = new BigDecimal(master.getAsNumber(WSCalculationConstant.UNIT_COST_CONST).toString());
		}
		return charge;
	}
	
	/**
	 * 
	 * @param masterData Master Data Map
	 * @param usageType - Property Usage Type
	 * @param noOfFlats
	 * @param connectionCatageory
	 * @param connectionType
	 * @param roadCuttingArea Road Cutting Area
	 * @return  returns UsageType Fee
	 */
	private BigDecimal getUsageTypeFee(Map<String, Object> masterData, String usageType, String connectionCatageory, String connectionType, Integer noOfFlats) {
		BigDecimal charge = BigDecimal.ZERO;
		JSONArray usageSlab = (JSONArray) masterData.getOrDefault(WSCalculationConstant.WC_PROPERTYUSAGETYPE_MASTER, null);
		JSONObject masterSlab = new JSONObject();
		// BigDecimal cuttingArea = new BigDecimal(roadCuttingArea.toString());
		if(usageSlab != null) {
			masterSlab.put("PropertyUsageType", usageSlab);
			String oldFilter = "$.PropertyUsageType[?(@.code=='"+usageType+"')]";
			String filter = "$.PropertyUsageType[?(@.code=='"+usageType+"' && @.connectionType=='"+connectionType+"' && @.connectionCatageory=='"+connectionCatageory+"' && @.noOfFlats[?(@.from <="+ noOfFlats +"&& @.to > " + noOfFlats + ")])]";
			//"$.PropertyUsageType.*." + connectionType + ".*." + connectionCatageory + ".*.[?(@.code=='"+usageType+"')].noOfFlats[?(@.from <="+ noOfFlats +"&& @.to > " + noOfFlats +")]";
			// $.PropertyUsageType[?(@.code=='"+usageType+"' && @.connectionType=='"+connectionType+"' && @.connectionCatageory=='"+connectionCatageory+"' && @.noOfFlats[?(@.from <="+ noOfFlats +"&& @.to > " + noOfFlats +]))];
			JSONArray filteredMasters = JsonPath.read(masterSlab, filter);
			if(CollectionUtils.isEmpty(filteredMasters))
				return charge;
			JSONObject master = mapper.convertValue(filteredMasters.get(0), JSONObject.class);
			charge = new BigDecimal(master.getAsNumber(WSCalculationConstant.UNIT_COST_CONST).toString());
			// charge = charge.multiply(cuttingArea);
		}
		return charge;
	}
	
	/**
	 * Enrich the adhoc penalty and adhoc rebate
	 * @param estimates tax head estimate
	 * @param connection water connection object
	 */
	@SuppressWarnings({ "unchecked"})
	private void addAdhocPenaltyAndRebate(List<TaxHeadEstimate> estimates, WaterConnection connection) {
		if (connection.getAdditionalDetails() != null) {
			HashMap<String, Object> additionalDetails = mapper.convertValue(connection.getAdditionalDetails(),
					HashMap.class);
			if (additionalDetails.getOrDefault(WSCalculationConstant.ADHOC_PENALTY, null) != null) {
				estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_ADHOC_PENALTY)
						.estimateAmount(
								new BigDecimal(additionalDetails.get(WSCalculationConstant.ADHOC_PENALTY).toString()))
						.build());
			}
			if (additionalDetails.getOrDefault(WSCalculationConstant.ADHOC_REBATE, null) != null) {
				estimates
						.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_ADHOC_REBATE)
								.estimateAmount(new BigDecimal(
										additionalDetails.get(WSCalculationConstant.ADHOC_REBATE).toString()).negate())
								.build());
			}
		}
	}

	public Map<String, List> getReconnectionFeeEstimation(CalculationCriteria criteria, RequestInfo requestInfo) {
		if (StringUtils.isEmpty(criteria.getWaterConnection()) && !StringUtils.isEmpty(criteria.getApplicationNo())) {
			SearchCriteria searchCriteria = new SearchCriteria();
			searchCriteria.setApplicationNumber(criteria.getApplicationNo());
			searchCriteria.setTenantId(criteria.getTenantId());
			WaterConnection waterConnection = calculatorUtil.getWaterConnectionOnApplicationNO(requestInfo, searchCriteria, requestInfo.getUserInfo().getTenantId());
			criteria.setWaterConnection(waterConnection);
		}
		if (StringUtils.isEmpty(criteria.getWaterConnection())) {
			throw new CustomException("WATER_CONNECTION_NOT_FOUND",
					"Water Connection are not present for " + criteria.getApplicationNo() + " Application no");
		}
		List<TaxHeadEstimate> taxHeadEstimates = getTaxHeadForReconnectionFeeEstimationV2(criteria, requestInfo);
		Map<String, List> estimatesAndBillingSlabs = new HashMap<>();
		estimatesAndBillingSlabs.put("estimates", taxHeadEstimates);
		return estimatesAndBillingSlabs;
	}

	private List<TaxHeadEstimate> getTaxHeadForReconnectionFeeEstimationV2(CalculationCriteria criteria,
			RequestInfo requestInfo) {
		BigDecimal reconnectionCharge = BigDecimal.ZERO;
		List<TaxHeadEstimate> estimates = new ArrayList<>();
		
		String usageCategory = criteria.getWaterConnection().getUsageCategory();
		BigDecimal scrutinyFee = BigDecimal.ZERO;
		if(criteria.getWaterConnection().getConnectionType().equalsIgnoreCase(WSCalculationConstant.meteredConnectionType)) {
			if(criteria.getWaterConnection().getConnectionCategory().equalsIgnoreCase("Permanent")) {
				switch(criteria.getWaterConnection().getUsageCategory().toUpperCase()) {
				case WSCalculationConstant.WS_UC_COMMERCIAL:
					if(criteria.getWaterConnection().getNoOfFlats() > 0 && criteria.getWaterConnection().getNoOfFlats() <= 25) {
						scrutinyFee = BigDecimal.valueOf(10000);
					} else if(criteria.getWaterConnection().getNoOfFlats() > 25 && criteria.getWaterConnection().getNoOfFlats() <= 50) {
						scrutinyFee = BigDecimal.valueOf(20000);
					} else if(criteria.getWaterConnection().getNoOfFlats() > 50) {
						scrutinyFee = BigDecimal.valueOf(30000);
					} else {
						scrutinyFee = BigDecimal.valueOf(6000);
					}
					break;
				case WSCalculationConstant.WS_UC_INDUSTRIAL:
					scrutinyFee = BigDecimal.valueOf(6000);
					break;
				case WSCalculationConstant.WS_UC_INSTITUTIONAL:
					scrutinyFee = BigDecimal.valueOf(5000);
					break;
				case WSCalculationConstant.WS_UC_DOMESTIC:
					if(criteria.getWaterConnection().getNoOfFlats() > 0 && criteria.getWaterConnection().getNoOfFlats() <= 25) {
						scrutinyFee = BigDecimal.valueOf(10000);
					} else if(criteria.getWaterConnection().getNoOfFlats() > 25 && criteria.getWaterConnection().getNoOfFlats() <= 50) {
						scrutinyFee = BigDecimal.valueOf(20000);
					} else if(criteria.getWaterConnection().getNoOfFlats() > 50) {
						scrutinyFee = BigDecimal.valueOf(30000);
					} else {
						scrutinyFee = BigDecimal.valueOf(3000);
					}
					break;
				case WSCalculationConstant.WS_UC_ASSOCIATION:
					scrutinyFee = BigDecimal.valueOf(6000);
				}
			} else if(criteria.getWaterConnection().getConnectionCategory().equalsIgnoreCase("Temporary")) {
				if(criteria.getWaterConnection().getNoOfFlats() > 0 && criteria.getWaterConnection().getNoOfFlats() <= 25) {
					scrutinyFee = BigDecimal.valueOf(10000);
				} else if(criteria.getWaterConnection().getNoOfFlats() > 25 && criteria.getWaterConnection().getNoOfFlats() <= 50) {
					scrutinyFee = BigDecimal.valueOf(20000);
				} else if(criteria.getWaterConnection().getNoOfFlats() > 50) {
					scrutinyFee = BigDecimal.valueOf(30000);
				} else if(WSCalculationConstant.WS_UC_TWSFC.equalsIgnoreCase(usageCategory)) {
					scrutinyFee = BigDecimal.valueOf(500);
				} else {
					scrutinyFee = BigDecimal.valueOf(3000);
				}
			}
		} else if(criteria.getWaterConnection().getConnectionType().equalsIgnoreCase(WSCalculationConstant.nonMeterdConnection)) {
			if(criteria.getWaterConnection().getConnectionCategory().equalsIgnoreCase("Permanent")) {
				switch(criteria.getWaterConnection().getUsageCategory().toUpperCase()) {
				case WSCalculationConstant.WS_UC_DOMESTIC:
					scrutinyFee = BigDecimal.valueOf(3000);
					break;
				}
			} else if(criteria.getWaterConnection().getConnectionCategory().equalsIgnoreCase("Temporary")) {
				if(WSCalculationConstant.WS_UC_DOMESTIC.equals(criteria.getWaterConnection().getUsageCategory().toUpperCase())) {
					scrutinyFee = BigDecimal.valueOf(3000);
				} else if(WSCalculationConstant.WS_UC_BPL.equals(criteria.getWaterConnection().getUsageCategory().toUpperCase())) {
					scrutinyFee = BigDecimal.valueOf(500);
				}
			}
		}
		reconnectionCharge = scrutinyFee.multiply(TenPercent).setScale(2, RoundingMode.UP);
		
		estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_RECONNECTION_CHARGE)
				.estimateAmount(reconnectionCharge.setScale(2, RoundingMode.UP)).build());
		
		return estimates;
	}

	public Map<String, List> getOwnershipChangeFeeEstimation(CalculationCriteria criteria, RequestInfo requestInfo) {
		if (StringUtils.isEmpty(criteria.getWaterConnection()) && !StringUtils.isEmpty(criteria.getApplicationNo())) {
			SearchCriteria searchCriteria = new SearchCriteria();
			searchCriteria.setApplicationNumber(criteria.getApplicationNo());
			searchCriteria.setTenantId(criteria.getTenantId());
			WaterConnection waterConnection = calculatorUtil.getWaterConnectionOnApplicationNO(requestInfo, searchCriteria, requestInfo.getUserInfo().getTenantId());
			criteria.setWaterConnection(waterConnection);
		}
		if (StringUtils.isEmpty(criteria.getWaterConnection())) {
			throw new CustomException("WATER_CONNECTION_NOT_FOUND",
					"Water Connection are not present for " + criteria.getApplicationNo() + " Application no");
		}
		List<TaxHeadEstimate> taxHeadEstimates = getTaxHeadForOwhershipChangeFeeEstimationV2(criteria, requestInfo);
		Map<String, List> estimatesAndBillingSlabs = new HashMap<>();
		estimatesAndBillingSlabs.put("estimates", taxHeadEstimates);
		return estimatesAndBillingSlabs;
	}

	private List<TaxHeadEstimate> getTaxHeadForOwhershipChangeFeeEstimationV2(CalculationCriteria criteria,
			RequestInfo requestInfo) {
		BigDecimal ownershipChangeFee = OWNERSHIP_CHANGE_FEE;
		
		List<TaxHeadEstimate> estimates = new ArrayList<>();
		if(ownershipChangeFee.compareTo(BigDecimal.ZERO) != 0) {
			estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_OWNERSHIP_CHANGE_FEE)
					.estimateAmount(ownershipChangeFee.setScale(2, RoundingMode.UP)).build());
		}
		
		return estimates;
	}
	
	public boolean isSpecialRebateApplicableForMonth(CalculationCriteria criteria, Map<String, Object> masterMap) {
		// TODO Auto-generated method stub
		@SuppressWarnings("unchecked")
		Map<String, Object> financialYearMaster = (Map<String, Object>) masterMap
				.get(WSCalculationConstant.BILLING_PERIOD);

		Long fromDate = (Long) financialYearMaster.get(WSCalculationConstant.STARTING_DATE_APPLICABLES);
		Long toDate = (Long) financialYearMaster.get(WSCalculationConstant.ENDING_DATE_APPLICABLES);

		LocalDate taxPeriodFrom = Instant.ofEpochMilli(fromDate).atZone(ZoneId.systemDefault()).toLocalDate();

		if(WSCalculationConstant.meteredConnectionType.equalsIgnoreCase(criteria.getWaterConnection().getConnectionType())) {
			 return Arrays.asList(configs.getSpecialRebateMonthsForMeteredConnection().trim().split(",")).stream()
				.filter(monYear ->
					Integer.parseInt(monYear.split("/")[0]) == taxPeriodFrom.getMonth().getValue() &&
							Integer.parseInt(monYear.split("/")[1]) == taxPeriodFrom.getYear()
				).count() > 0 ? true : false;
		} else {
			if(StringUtils.hasText(configs.getSpecialRebateYear()) && configs.getSpecialRebateYear().matches("\\d+") &&
					Integer.parseInt(configs.getSpecialRebateYear()) == taxPeriodFrom.getYear()) {
				if(StringUtils.hasText(configs.getSpecialRebateMonths())
						&& (Arrays.asList(configs.getSpecialRebateMonths().trim().split(","))).contains(String.valueOf(taxPeriodFrom.getMonth().getValue()))) {
					return true;
				}
			}
		}
		return false;
	}
	
	private BigDecimal calculateDiameterFixedCharge(Object additionalDetails) {
		BigDecimal sewerageCharge = BigDecimal.ZERO;
		LinkedHashMap additionalDetailJsonNode = (LinkedHashMap)additionalDetails;
		String diameter = null;
		if(additionalDetailJsonNode.containsKey("diameter")) {
			diameter = additionalDetailJsonNode.get("diameter").toString();
		}
		if(!StringUtils.isEmpty(diameter) && diameter.matches("\\d+")) {
			int dia = Integer.parseInt(diameter);
			if(dia == 4 ) {
				sewerageCharge = BigDecimal.valueOf(200);
			} else if(dia == 6 ) {
				sewerageCharge = BigDecimal.valueOf(500);
			} else if(dia == 8 ) {
				sewerageCharge = BigDecimal.valueOf(800);
			}
		}
		return sewerageCharge;
	}
	
	private BigDecimal getMeteredRate(CalculationCriteria criteria, String usageType) {
		BigDecimal rate = BigDecimal.ZERO;
		if(criteria.getWaterConnection().getConnectionCategory().equalsIgnoreCase("PERMANENT")) {
			if(usageType.equalsIgnoreCase("Domestic")) {
				rate = BigDecimal.valueOf(5.29);
			} else if(WSCalculationConstant.WS_UC_COMMERCIAL.equalsIgnoreCase(usageType)
					|| WSCalculationConstant.WS_UC_INDUSTRIAL.equalsIgnoreCase(usageType)
					|| WSCalculationConstant.WS_UC_INSTITUTIONAL.equalsIgnoreCase(usageType)) {
				rate = BigDecimal.valueOf(17.45);
			} else if(WSCalculationConstant.WS_UC_BPL.equalsIgnoreCase(usageType)) {
				rate = BigDecimal.valueOf(5.29);
			} else if(WSCalculationConstant.WS_UC_ASSOCIATION.equalsIgnoreCase(usageType)) {
				rate = BigDecimal.valueOf(17.45);
			}

		} else if(criteria.getWaterConnection().getConnectionCategory().equalsIgnoreCase("TEMPORARY")) {
			if(WSCalculationConstant.WS_UC_DOMESTIC.equalsIgnoreCase(usageType)) {
				rate = BigDecimal.valueOf(5.29);
			} else if(WSCalculationConstant.WS_UC_TWSFC.equalsIgnoreCase(usageType)) {
				rate = BigDecimal.valueOf(32.77);
			}
		}
		return rate;
	}
	
	private BigDecimal getDaysInMonth(Object object) {
		HashMap<String, Object> billPeriodMaster = (HashMap<String, Object>) object;
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis((long) billPeriodMaster.get(WSCalculationConstant.STARTING_DATE_APPLICABLES));
		int maxDays = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
		return BigDecimal.valueOf(maxDays);
	}
	
	/**
	 * Add installment amounts and taxheads for water connection
	 * @param waterConnection
	 * @param demand
	 */
	private void addTaxHeadsForInstallments(WaterConnection waterConnection, List<TaxHeadEstimate> estimates ) {

		List<Installments> installments = waterCalculatorDao.getApplicableInstallmentsByConsumerNo(waterConnection.getTenantId(), waterConnection.getConnectionNo());

		if(!installments.isEmpty()) {
			for (Installments installment : installments) {
				String taxHead = installment.getFeeType().concat("_INSTALLMENT");
				estimates.add(TaxHeadEstimate.builder().taxHeadCode(taxHead)
						.estimateAmount(installment.getAmount().setScale(2, 2)).build());
			}
		}
	}

	private BigDecimal applyLabourFeeInstallments(WaterConnection waterConnection, RequestInfo requestInfo, boolean isEstimate) {
		BigDecimal labourFee = BigDecimal.ZERO;
		int noOfLabourFeeInstallments = 0;

		if(waterConnection.getAdditionalDetails() == null) {
			return labourFee;
		}
		LinkedHashMap additionalDetailJsonNode = (LinkedHashMap)waterConnection.getAdditionalDetails();

		if(additionalDetailJsonNode.containsKey(WSCalculationConstant.NO_OF_LABOUR_FEE_INSTALLMENTS)
				&& !StringUtils.isEmpty(additionalDetailJsonNode.get(WSCalculationConstant.NO_OF_LABOUR_FEE_INSTALLMENTS))) {
			noOfLabourFeeInstallments = Integer.parseInt(additionalDetailJsonNode.get(WSCalculationConstant.NO_OF_LABOUR_FEE_INSTALLMENTS).toString());
		}
		if(additionalDetailJsonNode.containsKey(WSCalculationConstant.LABOUR_FEE_INSTALLMENT_AMOUNT)
				&& !StringUtils.isEmpty(additionalDetailJsonNode.get(WSCalculationConstant.LABOUR_FEE_INSTALLMENT_AMOUNT))) {
			labourFee = new BigDecimal(additionalDetailJsonNode.get(WSCalculationConstant.LABOUR_FEE_INSTALLMENT_AMOUNT).toString());
		}

		if(noOfLabourFeeInstallments > 0 && labourFee.compareTo(BigDecimal.ZERO) > 0) {
			if (!isEstimate) {
				int noOfRecords = waterCalculatorDao.getInstallmentCountByApplicationNoAndFeeType(waterConnection.getTenantId(), waterConnection.getApplicationNo(), WSCalculationConstant.WS_LABOUR_FEE);

				if(noOfRecords == 0) {
					populateAndCreateInstallments(waterConnection, requestInfo, noOfLabourFeeInstallments, labourFee,
							WSCalculationConstant.WS_LABOUR_FEE);
					// EMI will be applicable from first billing onwards 
					labourFee = BigDecimal.ZERO;
				}
			} else {
				labourFee = BigDecimal.ZERO;
			}
		}
		return labourFee;
	}

	/**
	 * Create installments for scrutiny fee if applicable
	 * @param waterConnection
	 * @param requestInfo
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	private BigDecimal applyScrutinyFeeInstallments(WaterConnection waterConnection, RequestInfo requestInfo, BigDecimal scrutinyFee, boolean isEstimate) {
//		BigDecimal scrutinyFee = BigDecimal.ZERO;
		boolean isInstallmentApplicableForScrutinyFee = Boolean.FALSE;
		int noOfScrutinyFeeInstallments = 0;

		if(waterConnection.getAdditionalDetails() == null) {
			return scrutinyFee;
		}
		LinkedHashMap additionalDetailJsonNode = (LinkedHashMap)waterConnection.getAdditionalDetails();
		if(additionalDetailJsonNode.containsKey(WSCalculationConstant.IS_INSTALLMENT_APPLICABLE_FOR_SCRUTINY_FEE)
				&& !StringUtils.isEmpty(additionalDetailJsonNode.get(WSCalculationConstant.IS_INSTALLMENT_APPLICABLE_FOR_SCRUTINY_FEE))) {
			isInstallmentApplicableForScrutinyFee = additionalDetailJsonNode.get(WSCalculationConstant.IS_INSTALLMENT_APPLICABLE_FOR_SCRUTINY_FEE).toString().equalsIgnoreCase("Y") 
					? Boolean.TRUE:Boolean.FALSE;
		}
		if(additionalDetailJsonNode.containsKey(WSCalculationConstant.NO_OF_SCRUTINY_FEE_INSTALLMENTS)
				&& !StringUtils.isEmpty(additionalDetailJsonNode.get(WSCalculationConstant.NO_OF_SCRUTINY_FEE_INSTALLMENTS))) {
			noOfScrutinyFeeInstallments = Integer.parseInt(additionalDetailJsonNode.get(WSCalculationConstant.NO_OF_SCRUTINY_FEE_INSTALLMENTS).toString());
		}
		if(additionalDetailJsonNode.containsKey(WSCalculationConstant.SCRUTINY_FEE_INSTALLMENT_AMOUNT)
				&& !StringUtils.isEmpty(additionalDetailJsonNode.get(WSCalculationConstant.SCRUTINY_FEE_INSTALLMENT_AMOUNT))) {
			scrutinyFee = new BigDecimal(additionalDetailJsonNode.get(WSCalculationConstant.SCRUTINY_FEE_INSTALLMENT_AMOUNT).toString());
		}

		if(isInstallmentApplicableForScrutinyFee && noOfScrutinyFeeInstallments > 0 && scrutinyFee.compareTo(BigDecimal.ZERO) > 0) {
			if (!isEstimate) {
				int noOfRecords = waterCalculatorDao.getInstallmentCountByApplicationNoAndFeeType(waterConnection.getTenantId(), waterConnection.getApplicationNo(), WSCalculationConstant.WS_SCRUTINY_FEE);
				if (noOfRecords == 0) {
					populateAndCreateInstallments(waterConnection, requestInfo, noOfScrutinyFeeInstallments,
							scrutinyFee, WSCalculationConstant.WS_SCRUTINY_FEE);
					// EMI will be applicable from first billing onwards
					scrutinyFee = BigDecimal.ZERO;
				}
			} else {
				scrutinyFee = BigDecimal.ZERO;
			}
		}

		return scrutinyFee;
	}

	/**
	 * Create and push {@link Installments} for creation
	 * @param waterconnection
	 * @param requestInfo
	 * @param noOfInstallments
	 * @param amount
	 * @param feeType
	 * @return
	 */
	private List<Installments> populateAndCreateInstallments(WaterConnection waterconnection, RequestInfo requestInfo,
			int noOfInstallments, BigDecimal amount, String feeType) {
		Long time = System.currentTimeMillis();
		List<Installments> installments = new ArrayList<>();
		Installments installment = null;
		for(int i=1;i<=noOfInstallments;i++) {
			installment = Installments.builder()
			.id(UUID.randomUUID().toString())
			.applicationNo(waterconnection.getApplicationNo())
			.tenantId(waterconnection.getTenantId())
			.consumerNo(waterconnection.getConnectionNo())
			.amount(amount).installmentNo(i).feeType(feeType)
			.auditDetails(AuditDetails.builder().createdBy(requestInfo.getUserInfo().getUuid())
						.lastModifiedBy(requestInfo.getUserInfo().getUuid()).createdTime(time)
						.lastModifiedTime(time).build())
			.build();
			installments.add(installment);
		}

		wsCalculationProducer.push(configs.getWsCreateInstallmentTopic(), InstallmentsRequest.builder().requestInfo(requestInfo).installments(installments).build());
		return installments;
	}
	
	public AnnualPaymentDetails getAnnualAdvanceEstimation(CalculationCriteria criteria, RequestInfo requestInfo,
			Map<String, Object> masterData) {
		String tenantId = criteria.getTenantId();
		if (criteria.getWaterConnection() == null && !StringUtils.isEmpty(criteria.getConnectionNo())) {
			List<WaterConnection> waterConnectionList = calculatorUtil.getWaterConnection(requestInfo, criteria.getConnectionNo(), tenantId);
			WaterConnection waterConnection = calculatorUtil.getWaterConnectionObject(waterConnectionList);
			criteria.setWaterConnection(waterConnection);
		}
		if (criteria.getWaterConnection() == null || StringUtils.isEmpty(criteria.getConnectionNo())) {
			StringBuilder builder = new StringBuilder();
			builder.append("Water Connection are not present for ")
					.append(StringUtils.isEmpty(criteria.getConnectionNo()) ? "" : criteria.getConnectionNo())
					.append(" connection no");
			throw new CustomException("WATER_CONNECTION_NOT_FOUND", builder.toString());
		}

		Map<String, JSONArray> billingSlabMaster = new HashMap<>();
		Map<String, JSONArray> timeBasedExemptionMasterMap = new HashMap<>();
		ArrayList<String> billingSlabIds = new ArrayList<>();
		billingSlabMaster.put(WSCalculationConstant.WC_BILLING_SLAB_MASTER,
				(JSONArray) masterData.get(WSCalculationConstant.WC_BILLING_SLAB_MASTER));
		billingSlabMaster.put(WSCalculationConstant.CALCULATION_ATTRIBUTE_CONST,
				(JSONArray) masterData.get(WSCalculationConstant.CALCULATION_ATTRIBUTE_CONST));
		timeBasedExemptionMasterMap.put(WSCalculationConstant.WC_WATER_CESS_MASTER,
				(JSONArray) (masterData.getOrDefault(WSCalculationConstant.WC_WATER_CESS_MASTER, null)));

		mDataService.setWaterConnectionMasterValues(requestInfo, criteria.getTenantId(), billingSlabMaster,
				timeBasedExemptionMasterMap);

		BigDecimal waterTaxAmt = getWaterEstimationChargeV2(criteria.getWaterConnection(), criteria, requestInfo, masterData);
		BigDecimal sewerageTaxAmt = getSewerageEstimationChargeV2(criteria.getWaterConnection(), criteria, requestInfo);

		AnnualPaymentDetails annualPaymentDetails = AnnualPaymentDetails.builder().tenantid(criteria.getTenantId()).consumerCode(criteria.getConnectionNo()).build();

		CalculateAnnualAdvance(annualPaymentDetails, waterTaxAmt, sewerageTaxAmt, timeBasedExemptionMasterMap.get(WSCalculationConstant.WC_ANNUAL_ADVANCE_MASTER));

		return annualPaymentDetails;
	}

	private void CalculateAnnualAdvance(AnnualPaymentDetails annualPaymentDetails, BigDecimal waterTaxAmt, BigDecimal sewerageTaxAmt,
			JSONArray annualAdvanceMaster) {


		BigDecimal increasedWaterTax = waterTaxAmt.add(waterTaxAmt.multiply(FivePercent).setScale(2, RoundingMode.HALF_UP));

		// TODO Auto-generated method stub
		int month = calculatorUtil.getTodayMonth();
		int noOfMonthsForCurrentCharges=0;
		int noOfMonthsForIncrementedCharges=0;
		if(month>=4 && month <=6 ) {
			noOfMonthsForCurrentCharges = 6 - month + 1;
			noOfMonthsForIncrementedCharges = 9;
		} else {
			if(month>=7 && month <=12 ) {
				noOfMonthsForIncrementedCharges = 12 - month + 4;
			} else {
				noOfMonthsForIncrementedCharges = 3 - month + 1;
			}
		}


		BigDecimal totalWaterTaxAmount = waterTaxAmt.multiply(BigDecimal.valueOf(noOfMonthsForCurrentCharges)).setScale(2, RoundingMode.HALF_UP)
									.add(increasedWaterTax.multiply(BigDecimal.valueOf(noOfMonthsForIncrementedCharges)).setScale(2, RoundingMode.HALF_UP));
		BigDecimal totalSewerageTaxAmount = sewerageTaxAmt.multiply(BigDecimal.valueOf(noOfMonthsForCurrentCharges+noOfMonthsForIncrementedCharges)).setScale(2, RoundingMode.HALF_UP);

		BigDecimal annualRebate = payService.getAnnualAdvanceRebate(totalWaterTaxAmount, totalSewerageTaxAmount, calculatorUtil.getFinancialYear(), annualAdvanceMaster).negate();
		BigDecimal netAnnualAdvancePayable = totalWaterTaxAmount.add(totalSewerageTaxAmount).add(annualRebate).setScale(0, RoundingMode.CEILING);

		annualPaymentDetails.setTotalWaterCharge(totalWaterTaxAmount);
		annualPaymentDetails.setTotalSewerageCharge(totalSewerageTaxAmount);
		annualPaymentDetails.setTotalRebate(annualRebate);
		annualPaymentDetails.setNetAnnualAdvancePayable(netAnnualAdvancePayable);
	}
}
