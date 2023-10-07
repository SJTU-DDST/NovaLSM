package com.yahoo.ycsb.validation;

/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/



import java.text.DecimalFormat;

public abstract class Constants {

	public static final String ESCAPE_START_CHAR = "\\(";
    public static final String ESCAPE_END_CHAR = "\\)";

    public static final int KEY_SEPERATOR_NUM = 1;
    public static final int RECORD_ATTRIBUTE_SEPERATOR_NUM = 2;
    public static final int ENTITY_SEPERATOR_NUM = 3;
    public static final int PROPERY_SEPERATOR_NUM = 4;
    public static final int PROPERY_ATTRIBUTE_SEPERATOR_NUM = 5;
    public static final int ENTITY_ATTRIBUTE_SEPERATOR_NUM = 6;
    public static final int ESCAPE_START_NUM = 7;
    public static final int ESCAPE_END_NUM = 8;
	public static final String TABLENAME_DISTRICT = "DISTRICT";
	public static final String TABLENAME_WAREHOUSE = "WAREHOUSE";
	public static final String TABLENAME_ITEM = "ITEM";
	public static final String TABLENAME_STOCK = "STOCK";
	public static final String TABLENAME_CUSTOMER = "CUSTOMER";
	public static final String TABLENAME_HISTORY = "HISTORY";
	public static final String TABLENAME_OPENORDER = "OORDER";
	public static final String TABLENAME_ORDERLINE = "ORDER_LINE";
	public static final String TABLENAME_NEWORDER = "NEW_ORDER";

	public static final String NEWORDER_ACTION = "NO";
	public static final String ORDERSTATUS_ACTION = "OS";
	public static final String PAYMENT_ACTION = "PA";
	public static final String DELIVERY_ACTION = "DE";
	public static final String BUCKET_ACTION = "BU";
	public static final String STOCKLEVEL_ACTION = "SL";

	public static final String CUSTOMER_ENTITY = "CUS";// "CUSTOMER";
	public static final String ORDER_ENTITY = "ORD"; // ER";
	public static final String DISTRICT_ENTITY = "DIS";// TRICT";
	public static final String STOCK_ENTITY = "STK";

	public static final char NEW_VALUE_UPDATE = 'N';
	public static final char INCREMENT_UPDATE = 'I';
	public static final char NO_READ_UPDATE = 'X';
	public static final char VALUE_READ = 'R';

	public static final char RECORD_ATTRIBUTE_SEPERATOR = ',';
	public static final char ENTITY_SEPERATOR = '&';
	public static final char PROPERY_SEPERATOR = '#';
	public static final char PROPERY_ATTRIBUTE_SEPERATOR = ':';
	public static final char ENTITY_ATTRIBUTE_SEPERATOR = ';';
	public static final char RELATIONSHIP_ENTITY_SEPERATOR = PROPERY_SEPERATOR;
	public static final char KEY_SEPERATOR = '-';
	public static final char UPDATE_RECORD = 'U';
	public static final char READ_RECORD = 'R';
	public static final long DELIVERYDATE = 1444665544000L;
	public static final double ERROR_MARGIN = 0.03;
	// public static final String BALANCE_DATA_ITEM = "BALANCE";
	// public static final String CARRID_DATA_ITEM = "CARRID";
	// public static final String PCNT_DATA_ITEM = "PCNT";
	// public static final String YTDP_DATA_ITEM = "YTDP";
	// public static final String OID_DATA_ITEM = "OID";
	// public static final String OLCNT_DATA_ITEM = "OLCNT";
	// public static final String OLDELD_DATA_ITEM = "OLDELD";

	// public static final char STRING = 's';
	// public static final char DBSTATE = 'd';

	public static final String ENTITY_SEPERATOR_REGEX = "[" + ENTITY_SEPERATOR + "]";
	public static final String ENTITY_ATTRIBUTE_SEPERATOR_REGEX = "[" + ENTITY_ATTRIBUTE_SEPERATOR + "]";
	public static final String PROPERY_SEPERATOR_REGEX = "[" + PROPERY_SEPERATOR + "]";
	public static final String PROPERY_ATTRIBUTE_SEPERATOR_REGEX = "[" + PROPERY_ATTRIBUTE_SEPERATOR + "]";

	public static final String CUSTOMER_BALANCE = "BALANCE";
	public static final String CUSTOMER_YTD_PAYMENT = "YTD_P";
	public static final String CUSTOMER_PAYMENT_COUNT = "P_CNT";

	public static final String ORDER_ORDER_COUNT = "OL_CNT";
	public static final String ORDER_CARRIER_ID = "CARRID";
	public static final String ORDER_DELIVERY_DATE = "OL_DEL_D";

	public static boolean ENABLE_LOGGING = true;
	public static String TRACE_LOGGING_DIR = "";
	public static boolean IQ = false;
	public static String CACHE_IP;
	public static int CACHE_PORT = 11211;
	public static boolean isCache = false;
	public static boolean compression = true;
	public static boolean DML_Trace = false;

	public static enum COType {
		NotSet, Invalidate, Refill;
	}

	public static COType coType = COType.Invalidate;
	public static int DELIVERY_DATE_DIVISION = 1000; // This used to discard digits from the delivery date in millis to match retrieved time stamp;
	public static DecimalFormat DECIMAL_FORMAT = new DecimalFormat("##.00");
	public static boolean FILE_LOG = true;
	public static final String[] CUST_ORDER_REL_PROPERIES = { CUSTOMER_ENTITY, ORDER_ENTITY };
	public static final String[] CUSTOMER_PROPERIES = { CUSTOMER_BALANCE, CUSTOMER_YTD_PAYMENT, CUSTOMER_PAYMENT_COUNT };
	// public static final char[] CUSTOMER_PROPERIES_TYPES = { STRING, STRING, STRING/* , DBSTATE */ };
	public static final String[] ORDER_PROPERIES = { ORDER_ORDER_COUNT, ORDER_CARRIER_ID, ORDER_DELIVERY_DATE };
	// public static final char[] ORDER_PROPERIES_TYPES = { STRING, STRING, STRING };
	public static final String[] DISTRICT_PROPERIES = { "N_O_ID" };
	public static final String[] STOCK_PROPERIES = { "QUANTITY" };

	public static final String CUST_ORDER_REL = CUSTOMER_ENTITY + "*" + ORDER_ENTITY;

	// public static final String[] ENTITY_NAMES = { CUSTOMER_ENTITY, ORDER_ENTITY, CUST_ORDER_REL };
	// public static final String[][] ENTITY_PROPERTIES = { CUSTOMER_PROPERIES, ORDER_PROPERIES };
	// public static final char[][] ENTITY_PROPERTIES_TYPES = { CUSTOMER_PROPERIES_TYPES, ORDER_PROPERIES_TYPES };
	public static String[][] ENTITIES_INSERT_ACTIONS;
	// =============================================

	public static final String MEMBER_ENTITY = "MEMBER";
	public static final String USER_ENTITY = "USR";
	public static final String[] FRIEND_COUNT_PROPERIES = { "FRIEND_CNT" };
	public static final String[] PENDING_COUNT_PROPERIES = { "PENDING_CNT" };

	public static final String[] MEMBER_PROPERIES = { "FRIEND_CNT", "PENDING_CNT" };
	// public static final char[] MEMBER_PROPERIES_TYPES = { STRING, STRING };

	public static final String INSERT_ACTION = "INSERT";
	public static final String READ_ACTION = "READ";
	public static final String MODIFY_ACTION = "MODIFY";
	public static final String UPDATE_ACTION = "UPDATE";
	public static final String DELETE_ACTION = "DELETE";
	public static final String SCAN_ACTION = "SCAN";

	public static final String NULL_STRING = "NULL";
	public static final char VALUE_NA = 'U';
	public static final char VALUE_DELETED = 'D';

	public static String[] ENTITY_NAMES = { USER_ENTITY };

	public static final String[] USER_PROPERIES = { "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10" };
	public static final String DELETED_STRING = "#DELETED#";

	public static String[][] ENTITY_PROPERTIES = { USER_PROPERIES };
	// public static char[][] ENTITY_PROPERTIES_TYPES = { MEMBER_PROPERIES_TYPES };

	public static String cacheKey(String template, Object... params) {
		int count = template.length() - template.replace("?", "").length();
		assert count == params.length : "Number of ? in the template (" + count + ") does not equal the number of parameters sent (" + params.length + ")";
		for (Object str : params) {
			template = template.replaceFirst("[?]", str.toString());
		}
		return template;
	}

	public static float fixFloat(float num) {
		// num = Float.valueOf(TPCCConstants.DECIMAL_FORMAT.format(num));
		return num;
	}

	public static final String DistrictKey = "Dist_?_?";
	public static final String CustomerKey = "Customer_?_?_?";
	public static final String OrderKey = "Order_?_?_?";
	public static final String StockKey = "Stock_?_?";
	public static final String LastOrderKey = "LastOrder_?_?_?";

	public static final String custwhseKey = "custwhse_?_?_?";
	public static final String PAWarehouseInfoKey = "WareInfo_?";
	public static final String PADistrictInfoKey = "DistInfo_?_?";
	public static final String ItemKey = "Item_?";

	public static final String D_NEXT_O_ID_STRING = "D_NEXT_O_ID";

	public static final long NullDate = 0L;// Long.MIN_VALUE;

	public static final boolean verbose = false;

	public static void printERROR(int threadId, String transactionName, String exception, String msg, String key, int lineNumber, String messege) {
		System.out.println(String.format("%d: %s: throw %s:[line %d] %s. key: %s, Messege: %s", threadId, transactionName, exception, lineNumber, msg, key, messege));
	}



}

