package org.egov.waterconnection.service;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.egov.common.contract.request.RequestInfo;
import org.egov.waterconnection.constants.WCConstants;
import org.egov.waterconnection.web.models.RoadCuttingInfo;
import org.egov.waterconnection.web.models.ValidatorResult;
import org.egov.waterconnection.web.models.WaterConnection;
import org.egov.waterconnection.web.models.WaterConnectionRequest;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class WaterFieldValidator implements WaterActionValidator {

	@Override
	public ValidatorResult validate(WaterConnectionRequest waterConnectionRequest, int reqType) {
		Map<String, String> errorMap = new HashMap<>();
		if (reqType == WCConstants.CREATE_APPLICATION) {
			handleCreateApplicationRequest(waterConnectionRequest, errorMap);
		}
		if (reqType == WCConstants.UPDATE_APPLICATION) {
			handleUpdateApplicationRequest(waterConnectionRequest, errorMap);
		}
		if(reqType == WCConstants.MODIFY_CONNECTION){
			handleModifyConnectionRequest(waterConnectionRequest, errorMap);
		}
		if (!errorMap.isEmpty())
			return new ValidatorResult(false, errorMap);
		return new ValidatorResult(true, errorMap);
	}
	
	private void handleCreateApplicationRequest(WaterConnectionRequest waterConnectionRequest,
			Map<String, String> errorMap) {
		if(!StringUtils.hasText(waterConnectionRequest.getWaterConnection().getConnectionFacility())) {
			errorMap.put("INVALID_WATER_CONNECTION_FACILITY", "Connection Facility should not be empty");
		}
	}

	private void handleUpdateApplicationRequest(WaterConnectionRequest waterConnectionRequest,
			Map<String, String> errorMap) {
		if (WCConstants.ACTIVATE_CONNECTION_CONST
				.equalsIgnoreCase(waterConnectionRequest.getWaterConnection().getProcessInstance().getAction())) {
			if (StringUtils.isEmpty(waterConnectionRequest.getWaterConnection().getConnectionFacility())) {
				errorMap.put("INVALID_CONNECTION_FACILITY", "Connection Facility should not be empty");
			}
			if (StringUtils.isEmpty(waterConnectionRequest.getWaterConnection().getConnectionType())) {
				errorMap.put("INVALID_WATER_CONNECTION_TYPE", "Connection type should not be empty");
			}
			if (StringUtils.hasText(waterConnectionRequest.getWaterConnection().getConnectionFacility())
					&& (WCConstants.SERVICE_WATER.equalsIgnoreCase(waterConnectionRequest.getWaterConnection().getConnectionFacility())
							|| WCConstants.SERVICE_WATER_SEWERAGE.equalsIgnoreCase(waterConnectionRequest.getWaterConnection().getConnectionFacility()))
					&& StringUtils.isEmpty(waterConnectionRequest.getWaterConnection().getWaterSource())) {
				errorMap.put("INVALID_WATER_SOURCE", "WaterConnection cannot be created  without water source");
			}
			
			// if(waterConnectionRequest.getWaterConnection().getRoadCuttingInfo() == null){
			// 	errorMap.put("INVALID_ROAD_INFO", "Road Cutting Information should not be empty");
			// }
			if(waterConnectionRequest.getWaterConnection().getRoadCuttingInfo() != null){
				for(RoadCuttingInfo roadCuttingInfo : waterConnectionRequest.getWaterConnection().getRoadCuttingInfo()){
					if(StringUtils.isEmpty(roadCuttingInfo.getRoadType())){
						errorMap.put("INVALID_ROAD_TYPE", "Road type should not be empty");
					}
				}
			}
			if (StringUtils.isEmpty(waterConnectionRequest.getWaterConnection().getConnectionExecutionDate())
					|| waterConnectionRequest.getWaterConnection().getConnectionExecutionDate() <= 0L) {
				errorMap.put("INVALID_CONNECTION_EXECUTION_DATE", "Connection execution date should not be empty");
			}
//			if (!StringUtils.isEmpty(waterConnectionRequest.getWaterConnection().getConnectionExecutionDate())
//					&& System.currentTimeMillis() > waterConnectionRequest.getWaterConnection().getConnectionExecutionDate()) {
//				errorMap.put("INVALID_CONNECTION_EXECUTION_DATE", "Connection execution date cannot be past");
//			}
		}
		if (WCConstants.APPROVE_CONNECTION_CONST
				.equalsIgnoreCase(waterConnectionRequest.getWaterConnection().getProcessInstance().getAction())) {

			// if(waterConnectionRequest.getWaterConnection().getRoadCuttingInfo() == null){
			// 	errorMap.put("INVALID_ROAD_INFO", "Road Cutting Information should not be empty");
			// }

			if(waterConnectionRequest.getWaterConnection().getRoadCuttingInfo() != null){
				for(RoadCuttingInfo roadCuttingInfo : waterConnectionRequest.getWaterConnection().getRoadCuttingInfo()){
					if(StringUtils.isEmpty(roadCuttingInfo.getRoadType())){
						errorMap.put("INVALID_ROAD_TYPE", "Road type should not be empty");
					}
					if(roadCuttingInfo.getRoadCuttingArea() == null){
						errorMap.put("INVALID_ROAD_CUTTING_AREA", "Road cutting area should not be empty");
					}
				}
			}
		}
	}
	
	private void handleModifyConnectionRequest(WaterConnectionRequest waterConnectionRequest, Map<String, String> errorMap){
		boolean isEmployee = waterConnectionRequest.getRequestInfo().getUserInfo().getRoles().stream().map(role -> role.getCode()).collect(Collectors.toList()).contains(WCConstants.ROLE_EMPLOYEE);
		if (WCConstants.APPROVE_CONNECTION
				.equalsIgnoreCase(waterConnectionRequest.getWaterConnection().getProcessInstance().getAction())) {
			if (StringUtils.isEmpty(waterConnectionRequest.getWaterConnection().getConnectionFacility())) {
				errorMap.put("INVALID_CONNECTION_FACILITY", "Connection Facility should not be empty");
			}
			if (StringUtils.isEmpty(waterConnectionRequest.getWaterConnection().getConnectionType())) {
				errorMap.put("INVALID_WATER_CONNECTION_TYPE", "Connection type should not be empty");
			}
			if (StringUtils.hasText(waterConnectionRequest.getWaterConnection().getConnectionFacility())
					&& (WCConstants.SERVICE_WATER.equalsIgnoreCase(waterConnectionRequest.getWaterConnection().getConnectionFacility())
							|| WCConstants.SERVICE_WATER_SEWERAGE.equalsIgnoreCase(waterConnectionRequest.getWaterConnection().getConnectionFacility()))
					&& StringUtils.isEmpty(waterConnectionRequest.getWaterConnection().getWaterSource())) {
				errorMap.put("INVALID_WATER_SOURCE", "WaterConnection cannot be created  without water source");
			}
			if (StringUtils.isEmpty(waterConnectionRequest.getWaterConnection().getConnectionExecutionDate())) {
				errorMap.put("INVALID_CONNECTION_EXECUTION_DATE", "Connection execution date should not be empty");
			}
//			if (!StringUtils.isEmpty(waterConnectionRequest.getWaterConnection().getConnectionExecutionDate())
//					&& System.currentTimeMillis() > waterConnectionRequest.getWaterConnection().getConnectionExecutionDate()) {
//				errorMap.put("INVALID_CONNECTION_EXECUTION_DATE", "Connection execution date cannot be past");
//			}
		}
		if ((WCConstants.SUBMIT_APPLICATION_CONST
				.equals(waterConnectionRequest.getWaterConnection().getProcessInstance().getAction())
				|| WCConstants.APPROVE_CONNECTION.equalsIgnoreCase(
				waterConnectionRequest.getWaterConnection().getProcessInstance().getAction()))
				&& isEmployee) {
			if (waterConnectionRequest.getWaterConnection().getDateEffectiveFrom() == null
					|| waterConnectionRequest.getWaterConnection().getDateEffectiveFrom() < 0
					|| waterConnectionRequest.getWaterConnection().getDateEffectiveFrom() == 0) {
				errorMap.put("INVALID_DATE_EFFECTIVE_FROM", "Date effective from cannot be null or negative");
			}
			if (waterConnectionRequest.getWaterConnection().getDateEffectiveFrom() != null) {
				if (System.currentTimeMillis() > waterConnectionRequest.getWaterConnection().getDateEffectiveFrom()) {
					errorMap.put("DATE_EFFECTIVE_FROM_IN_PAST", "Date effective from cannot be past");
				}
				if (waterConnectionRequest.getWaterConnection().getDateEffectiveFrom() != null) {
					if (System.currentTimeMillis() > waterConnectionRequest.getWaterConnection().getDateEffectiveFrom()) {
						errorMap.put("DATE_EFFECTIVE_FROM_IN_PAST", "Date effective from cannot be past");
					}
					if ((waterConnectionRequest.getWaterConnection().getConnectionExecutionDate() != null)
							&& (waterConnectionRequest.getWaterConnection()
							.getConnectionExecutionDate() > waterConnectionRequest.getWaterConnection()
							.getDateEffectiveFrom())) {

						errorMap.put("DATE_EFFECTIVE_FROM_LESS_THAN_EXCECUTION_DATE",
								"Date effective from cannot be before connection execution date");
					}
					if ((waterConnectionRequest.getWaterConnection().getMeterInstallationDate() != null)
							&& (waterConnectionRequest.getWaterConnection()
							.getMeterInstallationDate() > waterConnectionRequest.getWaterConnection()
							.getDateEffectiveFrom())) {
						errorMap.put("DATE_EFFECTIVE_FROM_LESS_THAN_METER_INSTALLATION_DATE",
								"Date effective from cannot be before meter installation date");
					}
					
					if (StringUtils.isEmpty(waterConnectionRequest.getWaterConnection().getConnectionFacility())) {
						errorMap.put("INVALID_CONNECTION_FACILITY", "Connection Facility should not be empty");
					}

				}
			}
		}
	}
}
