package com.memorynotfound.kafka.model;

public class Payload {
    String LoanNumber;
    String MappingSetID;
    String sourceEnv;
    String status;

    public String getLoanNumber() {
        return LoanNumber;
    }

    public void setLoanNumber(String loanNumber) {
        LoanNumber = loanNumber;
    }

    public String getMappingSetID() {
        return MappingSetID;
    }

    public void setMappingSetID(String mappingSetID) {
        MappingSetID = mappingSetID;
    }

    public String getSourceEnv() {
        return sourceEnv;
    }

    public void setSourceEnv(String sourceEnv) {
        this.sourceEnv = sourceEnv;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "Payload{" +
                "LoanNumber='" + LoanNumber + '\'' +
                ", MappingSetID='" + MappingSetID + '\'' +
                ", sourceEnv='" + sourceEnv + '\'' +
                ", status='" + status + '\'' +
                '}';
    }
}
