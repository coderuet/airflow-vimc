from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime


@dataclass
class CompaniesResponse:
    reportDate: datetime
    companyId: str
    companyName: str
    companyGroup: str
    taxCode: str
    capitalType: str
    unitName: str
    annualPlanIds: List[str]
    departmentIds: List[int]
    effectiveDate: str
    expirationDate: Optional[str]
    createdAt: str
    updatedAt: str


@dataclass
class DepartmentsResponse:
    reportDate: str
    departmentId: List[str]
    companyId: str
    departmentName: str
    parentDepartmentId: Optional[str]
    responsibleEmployeeId: Optional[str]
    positionIds: List[str]
    effectiveDate: str
    expirationDate: Optional[str]
    createdAt: str
    updatedAt: str


@dataclass
class PositionsResponse:
    reportDate: str
    positionId: int
    departmentId: int
    positionName: str
    employeeIds: List[str]
    effectiveDate: str
    expirationDate: Optional[str]
    isKeyPosition: bool
    isLeadershipPosition: bool
    isDeleted: bool
    isActive: bool
    isHazardousEnvironment: bool


@dataclass
class ShipDetailsResponse:
    reportDate: str
    shipId: str
    shipName: str
    shipGroup: str
    buildYear: int
    buildPlace: str
    shippingRegister: str
    flagState: str
    shipType: str
    deadweightTonnage: float
    shipOwner: str
    shipStatus: str
    createdDate: str
    modifiedDate: str


@dataclass
class GoodsResponse:
    reportDate: str
    goodsId: str
    goodsType: str
    goodsName: str
    calculationUnit: str
    createdDate: str
    modifiedDate: str


@dataclass
class ServiceResponse:
    reportDate: str
    serviceId: str
    serviceType: str
    serviceName: str


@dataclass
class HandlingMethodResponse:
    reportDate: str
    handlingMethodId: str
    handlingMethodName: str


@dataclass
class AnnualPlanCBResponse:
    reportDate: str
    annualPlanId: str
    companyId: str
    year: int
    monthlyPlanIds: List[str]
    plannedContainerTeu: float
    plannedNonContainerTonnage: float


@dataclass
class AnnualPlanVTBResponse:
    reportDate: str
    annualPlanId: str
    companyId: str
    year: int
    monthlyPlanIds: List[str]
    plannedContainerTeu: float
    plannedCargoThroughput: float
    plannedCargoTurnover: float


@dataclass
class VolumeItem:
    metric: str
    goodsId: Optional[str]
    plannedValue: float
    unit: str


@dataclass
class MonthlyPlanCBResponse:
    reportDate: str
    monthlyPlanId: str
    annualPlanId: str
    period: int
    volumes: List[VolumeItem]


@dataclass
class MonthlyPlanVTBResponse:
    reportDate: str
    monthlyPlanId: str
    annualPlanId: str
    period: int
    plannedContainerTeu: float
    plannedCargoThroughput: float
    plannedCargoTurnover: float


@dataclass
class JobGradeResponse:
    reportDate: str
    jobGradeId: str
    jobGradeName: str
    salaryLevel: int
    salaryMin: float
    salaryMax: float
    salaryStep: float
    description: str
    isActive: bool
    effectiveDate: str
    expirationDate: Optional[str]
    companyId: str
