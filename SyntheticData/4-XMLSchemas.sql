USE [TestDb];
go


create xml schema collection TestDbXmlSchemaCollection as N'<?xml version="1.0" encoding="UTF-16"?>
<xsd:schema
    targetNamespace="http://schemas.microsoft.com/pz17/TestDb/Type1"
    xmlns="http://schemas.microsoft.com/pz17/TestDb/Type1"
    elementFormDefault="qualified"
    attributeFormDefault="unqualified" 
    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
>
    <xsd:complexType name="SymbolType" mixed="true">
		<xsd:sequence>
			<xsd:element name="Ticker" type="xsd:string" />
			<xsd:element name="Exchange" type="xsd:string" />
			<xsd:element name="Qty" type="xsd:decimal" />
		</xsd:sequence>
    </xsd:complexType>

    <xsd:element name="Transaction">
		<xsd:complexType mixed="true">
			<xsd:sequence>
				<xsd:element name="TransactionGuid" type="xsd:string" />
				<xsd:element name="Amount" type="xsd:decimal" />
				<xsd:element name="Settled" type="xsd:boolean" />
				<xsd:element name="TransactionDate" type="xsd:dateTime" />
				<xsd:element name="Note" type="xsd:string" />
				<xsd:element name="Symbols">
					<xsd:complexType mixed="true">
						<xsd:sequence>
							<xsd:element name="Symbol" type="SymbolType" minOccurs="1" maxOccurs="20" />
						</xsd:sequence>
					</xsd:complexType>
				</xsd:element>
			</xsd:sequence>
		</xsd:complexType>
    </xsd:element>
</xsd:schema>
<xsd:schema
    targetNamespace="http://schemas.microsoft.com/pz17/TestDb/Type2"
    xmlns="http://schemas.microsoft.com/pz17/TestDb/Type2"
    elementFormDefault="qualified"
    attributeFormDefault="unqualified"
    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
>
	<xsd:complexType name="OrderLineType" mixed="true">
		<xsd:sequence>
			<xsd:element name="OrderLineId" type="xsd:int" />
			<xsd:element name="OrderLineGuid" type="xsd:string" />
			<xsd:element name="OrderId" type="xsd:int" />
			<xsd:element name="ProductId" type="xsd:int" />
			<xsd:element name="Qty" type="xsd:decimal" />
			<xsd:element name="UnitPrice" type="xsd:decimal" />
			<xsd:element name="Discount" type="xsd:decimal" />
			<xsd:element name="DateCreated" type="xsd:dateTime" />
			<xsd:element name="DateUpdated" type="xsd:dateTime" />
		</xsd:sequence>
	</xsd:complexType>

	<xsd:complexType name="OrderType" mixed="true">
		<xsd:sequence>
			<xsd:element name="OrderId" type="xsd:int" />
			<xsd:element name="UserId" type="xsd:int" />
			<xsd:element name="OrderGuid" type="xsd:string" />
			<xsd:element name="OrderName" type="xsd:string" />
			<xsd:element name="InvoiceNumber" type="xsd:string" />
			<xsd:element name="PONumber" type="xsd:string" />
			<xsd:element name="DateCreated" type="xsd:dateTime" />
			<xsd:element name="DateUpdated" type="xsd:dateTime" />
			<xsd:element name="OrderLines">
				<xsd:complexType mixed="true">
					<xsd:sequence>
						<xsd:element name="OrderLine" type="OrderLineType" minOccurs="1" maxOccurs="100" />
					</xsd:sequence>
				</xsd:complexType>
			</xsd:element>
		</xsd:sequence>
	</xsd:complexType>

	<xsd:element name="Activity">
		<xsd:complexType mixed="true">
			<xsd:sequence>
				<xsd:element name="Orders">
					<xsd:complexType mixed="true">
						<xsd:sequence>
							<xsd:element name="Order" type="OrderType" minOccurs="1" maxOccurs="10" />
						</xsd:sequence>
					</xsd:complexType>
				</xsd:element>
			</xsd:sequence>
		</xsd:complexType>
	</xsd:element>
</xsd:schema>
'
;
go

-- Verify - list of collections in the database.
SELECT  *
FROM    sys.xml_schema_collections;
go

SELECT  *
FROM    sys.xml_schema_namespaces;
go
  
DROP XML SCHEMA COLLECTION TestDbXmlSchemaCollection;
go