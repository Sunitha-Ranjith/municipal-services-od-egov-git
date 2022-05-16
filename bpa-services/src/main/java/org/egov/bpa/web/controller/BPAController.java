package org.egov.bpa.web.controller;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import javax.validation.Valid;

import org.egov.bpa.service.BPAService;
import org.egov.bpa.util.BPAConstants;
import org.egov.bpa.util.BPAErrorConstants;
import org.egov.bpa.util.BPAUtil;
import org.egov.bpa.util.ResponseInfoFactory;
import org.egov.bpa.web.model.BPA;
import org.egov.bpa.web.model.BPARequest;
import org.egov.bpa.web.model.BPAResponse;
import org.egov.bpa.web.model.BPASearchCriteria;
import org.egov.bpa.web.model.DigitalSignCertificateResponse;
import org.egov.bpa.web.model.DscDetails;
import org.egov.bpa.web.model.RequestInfoWrapper;
import org.egov.tracer.model.CustomException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/bpa")
public class BPAController {

	@Autowired
	private BPAService bpaService;

	@Autowired
	private BPAUtil bpaUtil;

	@Autowired
	private ResponseInfoFactory responseInfoFactory;

	@PostMapping(value = "/_create")
	public ResponseEntity<BPAResponse> create(@Valid @RequestBody BPARequest bpaRequest) {
		bpaUtil.defaultJsonPathConfig();
		BPA bpa = bpaService.create(bpaRequest);
		List<BPA> bpas = new ArrayList<BPA>();
		bpas.add(bpa);
		BPAResponse response = BPAResponse.builder().BPA(bpas)
				.responseInfo(responseInfoFactory.createResponseInfoFromRequestInfo(bpaRequest.getRequestInfo(), true))
				.build();
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@PostMapping(value = "/_update")
	public ResponseEntity<BPAResponse> update(@Valid @RequestBody BPARequest bpaRequest) {
		BPA bpa = bpaService.update(bpaRequest);
		List<BPA> bpas = new ArrayList<BPA>();
		bpas.add(bpa);
		BPAResponse response = BPAResponse.builder().BPA(bpas)
				.responseInfo(responseInfoFactory.createResponseInfoFromRequestInfo(bpaRequest.getRequestInfo(), true))
				.build();
		return new ResponseEntity<>(response, HttpStatus.OK);

	}

	@PostMapping(value = "/_search")
	public ResponseEntity<BPAResponse> search(@Valid @RequestBody RequestInfoWrapper requestInfoWrapper,
			@Valid @ModelAttribute BPASearchCriteria criteria) {

		List<BPA> bpas = bpaService.search(criteria, requestInfoWrapper.getRequestInfo());

		BPAResponse response = BPAResponse.builder().BPA(bpas).responseInfo(
				responseInfoFactory.createResponseInfoFromRequestInfo(requestInfoWrapper.getRequestInfo(), true))
				.build();
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@PostMapping(value = "/_permitorderedcr")
	public ResponseEntity<Resource> getPdf(@Valid @RequestBody BPARequest bpaRequest) {

		Path path = Paths.get(BPAConstants.EDCR_PDF);
		Resource resource = null;

		bpaService.getEdcrPdf(bpaRequest);
		try {
			resource = new UrlResource(path.toUri());
		} catch (Exception ex) {
			throw new CustomException(BPAErrorConstants.UNABLE_TO_DOWNLOAD, "Unable to download the file");
		}

		return ResponseEntity.ok()
				.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + BPAConstants.EDCR_PDF + "\"")
				.body(resource);
	}
	
	@PostMapping(value = {"/_updatedscdetails"})
    public ResponseEntity<BPAResponse> updateDscDetails(@Valid @RequestBody BPARequest bpaRequest) {
        BPA bpa = bpaService.updateDscDetails(bpaRequest);
        List<BPA> bpas=new ArrayList<>();
        bpas.add(bpa);

        BPAResponse response = BPAResponse.builder().BPA(bpas).responseInfo(
                responseInfoFactory.createResponseInfoFromRequestInfo(bpaRequest.getRequestInfo(), true))
                .build();
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
	
	@PostMapping(value = { "/_searchdscdetails" })
	public ResponseEntity<DigitalSignCertificateResponse> searchDscDetails(
			@Valid @RequestBody RequestInfoWrapper requestInfoWrapper,
			@Valid @ModelAttribute BPASearchCriteria criteria, @RequestHeader HttpHeaders headers) {
		List<DscDetails> dscDetails = bpaService.searchDscDetails(criteria, requestInfoWrapper.getRequestInfo());

		DigitalSignCertificateResponse response = DigitalSignCertificateResponse.builder().dscDetails(dscDetails)
				.responseInfo(responseInfoFactory.createResponseInfoFromRequestInfo(requestInfoWrapper.getRequestInfo(),
						true))
				.build();
		return new ResponseEntity<>(response, HttpStatus.OK);

	}
	
	/**
	 * Wrapper API to bpa-calculator /_estimate API as
	 * cannot access bpa-calculator APIs from UI directly
	 * 
	 * @param bpaReq The calculation Request
	 * @return Calculation Response
	 */
	@PostMapping(value = { "/_estimate" })
	public ResponseEntity<Object> getFeeEstimate(@RequestBody Object bpaRequest) {
		Object response = bpaService.getFeeEstimateFromBpaCalculator(bpaRequest);
		return new ResponseEntity<>(response, HttpStatus.OK);
	}
	
	@PostMapping(value = { "/_mergeScrutinyReportToPermit" })
	public ResponseEntity<Object> mergeScrutinyReportToPermit(@Valid @RequestBody RequestInfoWrapper requestInfoWrapper,
			@Valid @RequestBody BPARequest bpaRequest) {
		return new ResponseEntity<>(
				bpaService.mergeScrutinyReportToPermit(bpaRequest, requestInfoWrapper.getRequestInfo()), HttpStatus.OK);
	}
	
	@PostMapping(value = "/_get")
	public ResponseEntity<BPAResponse> reportingSearch(@Valid @RequestBody RequestInfoWrapper requestInfoWrapper) {

		List<BPA> bpas = bpaService.searchApplications(requestInfoWrapper.getRequestInfo());

		BPAResponse response = BPAResponse.builder().BPA(bpas).responseInfo(
				responseInfoFactory.createResponseInfoFromRequestInfo(requestInfoWrapper.getRequestInfo(), true))
				.build();
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

}
