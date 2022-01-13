package org.egov.wscalculation.service;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.YearMonth;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;

import org.egov.common.contract.request.RequestInfo;
import org.egov.tracer.model.CustomException;
import org.egov.wscalculation.config.WSCalculationConfiguration;
import org.egov.wscalculation.constants.WSCalculationConstant;
import org.egov.wscalculation.util.CalculatorUtil;
import org.egov.wscalculation.util.WSCalculationUtil;
import org.egov.wscalculation.util.WaterCessUtil;
import org.egov.wscalculation.web.models.BillingSlab;
import org.egov.wscalculation.web.models.CalculationCriteria;
import org.egov.wscalculation.web.models.MeterReading;
import org.egov.wscalculation.web.models.MeterReadingSearchCriteria;
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
	
	private static BigDecimal OWNERSHIP_CHANGE_FEE = BigDecimal.valueOf(60);
	private static BigDecimal TenPercent = BigDecimal.valueOf(0.1);

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
		// Enrich criteria with meter reading
		if(WSCalculationConstant.meteredConnectionType.equalsIgnoreCase(criteria.getWaterConnection().getConnectionType())) {
			MeterReadingSearchCriteria meterSearchCriteria = MeterReadingSearchCriteria.builder().tenantId(tenantId).connectionNos(Stream.of(criteria.getConnectionNo()).collect(Collectors.toSet())).build();
			List<MeterReading> meterReadingLists = meterService.searchMeterReadings(meterSearchCriteria, requestInfo);
			criteria.setMeterReadingLists(meterReadingLists);
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
		mDataService.enrichBillingPeriod(criteria, billingFrequencyMap, masterData);
		// mDataService.setWaterConnectionMasterValues(requestInfo, tenantId,
		// billingSlabMaster,
		// timeBasedExemptionMasterMap);
		// BigDecimal taxAmt = getWaterEstimationCharge(criteria.getWaterConnection(), criteria, billingSlabMaster, billingSlabIds,
		// 		requestInfo);
		BigDecimal waterTaxAmt = getWaterEstimationChargeV2(criteria.getWaterConnection(), criteria, requestInfo, masterData);
		BigDecimal sewerageTaxAmt = getSewerageEstimationChargeV2(criteria.getWaterConnection(), criteria, requestInfo);
		List<TaxHeadEstimate> taxHeadEstimates = getEstimatesForTax(waterTaxAmt, sewerageTaxAmt, criteria.getWaterConnection(),
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
			//Double totalUnit = criteria.getCurrentReading() - criteria.getLastReading();
			Double totalUnit = calculateTotalUnit(criteria, masterData);
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
						waterCharges.add(BigDecimal.valueOf(35.18).multiply(BigDecimal.valueOf(criteria.getWaterConnection().getNoOfTaps() - 2)));
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
	
	private Double calculateTotalUnit(CalculationCriteria criteria, Map<String, Object> masterData) {
		@SuppressWarnings("unchecked")
		Map<String, Object> financialYearMaster = (Map<String, Object>) masterData.get(WSCalculationConstant.BILLING_PERIOD);

		Long fromDate = (Long) financialYearMaster.get(WSCalculationConstant.STARTING_DATE_APPLICABLES);
		Long toDate = (Long) financialYearMaster.get(WSCalculationConstant.ENDING_DATE_APPLICABLES);
		LocalDate billingStartDate = Instant.ofEpochMilli(fromDate).atZone(ZoneId.systemDefault()).toLocalDate();
		LocalDate billingEndDate = Instant.ofEpochMilli(toDate).atZone(ZoneId.systemDefault()).toLocalDate();
		
		return criteria.getMeterReadingLists().stream().filter(meterReading -> {
					LocalDate readingDate = Instant.ofEpochMilli(meterReading.getCurrentReadingDate()).atZone(ZoneId.systemDefault()).toLocalDate();
					return readingDate.compareTo(billingStartDate) >= 0 && readingDate.compareTo(billingEndDate) <= 0;
				})
				.mapToDouble(meterReading -> meterReading.getCurrentReading() - meterReading.getLastReading())
				.sum();
	}

	private BigDecimal getSewerageEstimationChargeV2(WaterConnection waterConnection, CalculationCriteria criteria,
			RequestInfo requestInfo) {
		
		BigDecimal sewerageCharge = BigDecimal.ZERO;
		if(WSCalculationConstant.MDMS_WATER_CONNECTION.equalsIgnoreCase(criteria.getWaterConnection().getConnectionFacility())) {
			return sewerageCharge;
		}
		
		if(configs.isSwMigratedDemandValueEnabled()) {
			int swDemandMonth = configs.getSwdemandMonthsCount();
			LinkedHashMap additionalDetails = (LinkedHashMap)criteria.getWaterConnection().getAdditionalDetails();
			BigDecimal migratedSewerageFee = BigDecimal.ZERO;
			if(additionalDetails.containsKey("migratedSewerageFee")) {
				migratedSewerageFee = new BigDecimal(additionalDetails.get("migratedSewerageFee").toString());
			}

			return migratedSewerageFee.multiply(BigDecimal.valueOf(swDemandMonth)).setScale(2, RoundingMode.HALF_UP);
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
		return sewerageCharge;
	
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
			WaterConnection connection,
			Map<String, JSONArray> timeBasedExemptionsMasterMap, RequestInfoWrapper requestInfoWrapper,
			Map<String, Object> masterData) {
		List<TaxHeadEstimate> estimates = new ArrayList<>();
		if(WSCalculationConstant.MDMS_WATER_CONNECTION.equalsIgnoreCase(connection.getConnectionFacility())
				|| WSCalculationConstant.MDMS_WATER_SEWERAGE_CONNECTION.equalsIgnoreCase(connection.getConnectionFacility())) {
			// water_charge
			estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_CHARGE)
					.estimateAmount(waterCharge.setScale(2, 2)).build());
		}
		
		if(WSCalculationConstant.MDMS_SEWERAGE_CONNECTION.equalsIgnoreCase(connection.getConnectionFacility())
				|| WSCalculationConstant.MDMS_WATER_SEWERAGE_CONNECTION.equalsIgnoreCase(connection.getConnectionFacility())) {
			// sewerage_charge
			estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.SW_CHARGE)
					.estimateAmount(SewerageCharge.setScale(2, 2)).build());
		}
		
		BigDecimal totalCharge = waterCharge.add(SewerageCharge);
		
		// water timebase Rebate
		BigDecimal timeBaseRebate = payService.getApplicableRebateForInitialDemand(totalCharge.setScale(2, 2), getAssessmentYear(), timeBasedExemptionsMasterMap.get(WSCalculationConstant.WC_REBATE_MASTER));
		if(timeBaseRebate.compareTo(BigDecimal.ZERO) > 0) {
			estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_TIME_REBATE)
					.estimateAmount(timeBaseRebate.setScale(2, 2).negate()).build());
		}
		
		// water Special Rebate
		if(isSpecialRebateApplicableForMonth(masterData)) {
			BigDecimal specialRebate = payService.getApplicableSpecialRebate(totalCharge.setScale(2, 2), getAssessmentYear(), timeBasedExemptionsMasterMap.get(WSCalculationConstant.WC_REBATE_MASTER));
			if(specialRebate.compareTo(BigDecimal.ZERO) > 0) {
				estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_SPECIAL_REBATE)
						.estimateAmount(specialRebate.setScale(2, 2).negate()).build());
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
		
		return estimates;
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
	
	public String getAssessmentYear() {
		LocalDateTime localDateTime = LocalDateTime.now();
		int currentMonth = localDateTime.getMonthValue();
		String assessmentYear;
		if (currentMonth >= Month.APRIL.getValue()) {
			assessmentYear = YearMonth.now().getYear() + "-";
			assessmentYear = assessmentYear
					+ (Integer.toString(YearMonth.now().getYear() + 1).substring(2, assessmentYear.length() - 1));
		} else {
			assessmentYear = YearMonth.now().getYear() - 1 + "-";
			assessmentYear = assessmentYear
					+ (Integer.toString(YearMonth.now().getYear()).substring(2, assessmentYear.length() - 1));

		}
		return assessmentYear;
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
			Map<String, Object> masterData) {
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
		List<TaxHeadEstimate> taxHeadEstimates = getTaxHeadForFeeEstimationV2(criteria, requestInfo, masterData);
		Map<String, List> estimatesAndBillingSlabs = new HashMap<>();
		estimatesAndBillingSlabs.put("estimates", taxHeadEstimates);
		// //Billing slab id
		estimatesAndBillingSlabs.put("billingSlabIds", billingSlabIds);
		return estimatesAndBillingSlabs;
	}

	private List<TaxHeadEstimate> getTaxHeadForFeeEstimationV2(CalculationCriteria criteria, RequestInfo requestInfo, Map<String, Object> masterData) {
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
			taxHeadEstimate.addAll(getWaterTaxHeadForFeeEstimationV2(criteria, requestInfo, masterData));
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
		estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.SW_SECURITY_CHARGE)
				.estimateAmount(securityCharge.setScale(2, 2)).build());

		return estimates;
	}

	private List<TaxHeadEstimate> getWaterTaxHeadForFeeEstimationV2(CalculationCriteria criteria, RequestInfo requestInfo, Map<String, Object> masterData) {

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
						} else if(WSCalculationConstant.WS_UC_BPL.equals(criteria.getWaterConnection().getUsageCategory().toUpperCase())) {
							scrutinyFee = BigDecimal.valueOf(500);
							securityCharge = BigDecimal.valueOf(60);
						}
					}
				}

				BigDecimal labourFee = calculateLabourFee(criteria.getWaterConnection().getAdditionalDetails(), masterData);
				
				List<TaxHeadEstimate> estimates = new ArrayList<>();
				estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_SCRUTINY_FEE)
						.estimateAmount(scrutinyFee.setScale(2, 2)).build());
				estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_SECURITY_CHARGE)
						.estimateAmount(securityCharge.setScale(2, 2)).build());
				
				if(labourFee.compareTo(BigDecimal.ZERO) > 0) {
					estimates.add(TaxHeadEstimate.builder().taxHeadCode(WSCalculationConstant.WS_LABOUR_FEE)
							.estimateAmount(labourFee.setScale(2, 2)).build());
				}
				
				return estimates;
	
	}
	private BigDecimal calculateLabourFee(Object additionalDetails, Map<String, Object> masterData) {
		BigDecimal labourFee = BigDecimal.ZERO;
		boolean isLabourFeeApplicable = Boolean.FALSE;
		boolean isInstallmentApplicable = Boolean.FALSE;
		if(additionalDetails == null) {
			return labourFee;
		}
		LinkedHashMap additionalDetailJsonNode = (LinkedHashMap)additionalDetails;
		if(additionalDetailJsonNode.containsKey(WSCalculationConstant.IS_LABOUR_FEE_APPLICABLE)) {
			isLabourFeeApplicable = additionalDetailJsonNode.get(WSCalculationConstant.IS_LABOUR_FEE_APPLICABLE).toString().equalsIgnoreCase("Y") 
					? Boolean.TRUE:Boolean.FALSE;
		}

		if(additionalDetailJsonNode.containsKey(WSCalculationConstant.IS_INSTALLMENT_APPLICABLE)) {
			isInstallmentApplicable = additionalDetailJsonNode.get(WSCalculationConstant.IS_INSTALLMENT_APPLICABLE).toString().equalsIgnoreCase("Y") 
					? Boolean.TRUE:Boolean.FALSE;
		}

		JSONArray labourFeeMaster = (JSONArray) masterData.getOrDefault(WSCalculationConstant.WC_LABOURFEE_MASTER, null);
		if (labourFeeMaster == null)
			throw new CustomException("LABOUR_FEE_NOT_FOUND", "labour fee master data not found!!");

		JSONObject labourFeeObj = mapper.convertValue(labourFeeMaster.get(0), JSONObject.class);

		if(isLabourFeeApplicable && isInstallmentApplicable) {
			//TODO: installment Calculation

		} else if(isLabourFeeApplicable && !isInstallmentApplicable) {
			if (labourFeeObj.get(WSCalculationConstant.LABOURFEE_TOTALAMOUNT) != null) {
				labourFee = new BigDecimal(labourFeeObj.getAsNumber(WSCalculationConstant.LABOURFEE_TOTALAMOUNT).toString());
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
	
	public boolean isSpecialRebateApplicableForMonth(Map<String, Object> masterMap) {
		// TODO Auto-generated method stub
		@SuppressWarnings("unchecked")
		Map<String, Object> financialYearMaster = (Map<String, Object>) masterMap
				.get(WSCalculationConstant.BILLING_PERIOD);

		Long fromDate = (Long) financialYearMaster.get(WSCalculationConstant.STARTING_DATE_APPLICABLES);
		Long toDate = (Long) financialYearMaster.get(WSCalculationConstant.ENDING_DATE_APPLICABLES);

		LocalDate taxPeriodFrom = Instant.ofEpochMilli(fromDate).atZone(ZoneId.systemDefault()).toLocalDate();

		if(StringUtils.hasText(configs.getSpecialRebateYear()) && configs.getSpecialRebateYear().matches("\\d+") &&
				Integer.parseInt(configs.getSpecialRebateYear()) == taxPeriodFrom.getYear()) {
			if(StringUtils.hasText(configs.getSpecialRebateMonths())
					&& (Arrays.asList(configs.getSpecialRebateMonths().trim().split(","))).contains(String.valueOf(taxPeriodFrom.getMonth().getValue()))) {
				return true;
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
}
