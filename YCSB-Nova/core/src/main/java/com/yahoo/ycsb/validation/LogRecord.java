package com.yahoo.ycsb.validation;

import java.util.Comparator;

public class LogRecord implements Comparable<LogRecord> {
	private String id;
	private String actionName;
	private long startTime;
	private long endTime;
	private char type;
	private Entity[] entities;
	private String partitionID = "0";
	private short groupNum;
	private long offset;

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	enum LogRecordToken {
		Type(0), Name(1), ThreadID(2), SequenceID(3), StartTime(4), EndTime(5), Entities(6);
		public final int Index;

		LogRecordToken(int index) {
			Index = index;
		}
	}

	enum EntityToken {
		Name(0), Key(1), Property(2);
		public final int Index;

		EntityToken(int index) {
			Index = index;
		}
	}

	enum PropertyToken {
		Name(0), Value(1), Type(2);
		public final int Index;

		PropertyToken(int index) {
			Index = index;
		}
	}

	public LogRecord(String id, String actionName, long startTime, long endTime, char type, Entity[] entities) {
		super();
		this.id = id;
		this.actionName = actionName;
		this.startTime = startTime;
		this.endTime = endTime;
		this.type = type;
		this.entities = entities;
		this.groupNum = -1;
	}

	public short getGroupNum() {
		return groupNum;
	}

	public void setGroupNum(short groupNum) {
		this.groupNum = groupNum;
	}

	private LogRecord() {

	}

	public static LogRecord createLogRecord(String line) {
		// R,OS,0,2,7535426455819328,7535426459286936,CUSTOMER;1-3-154;BALANCE:-10.0:R#YTD_P:10.0:R#P_CNT:1:R#LAST_O_ID:2564:R&ORDER;1-3-154-2564;OL_CNT:8:R#CARRID:0:R#OL_DEL_D:1444665544:R
		// U,NO,0,3,7535426460057615,7535426506998154,ORDER;1-2-689-3001;OL_CNT:3001:N#CARRID:6:N#OL_DEL_D:0:N&DISTRICT;1-2;N_O_ID:1:I&STOCK;1-96471;QUANTITY:87:N&STOCK;1-7459;QUANTITY:27:N&STOCK;1-26719;QUANTITY:67:N&STOCK;1-13925;QUANTITY:56:N&STOCK;1-15847;QUANTITY:18:N&STOCK;1-26531;QUANTITY:90:N
		LogRecord result = new LogRecord();
		String[] tokens = line.split("[" + Constants.RECORD_ATTRIBUTE_SEPERATOR + "]");
		int added=0;
		if (tokens.length >7){
			added=1;
		}
		try {
			result.type = tokens[LogRecordToken.Type.Index].charAt(0);
			result.actionName = tokens[LogRecordToken.Name.Index];
			result.id = Utilities.concat(tokens[LogRecordToken.ThreadID.Index], Constants.KEY_SEPERATOR, tokens[LogRecordToken.SequenceID.Index]);
			// result.id = tokens[LogRecordToken.ThreadID.Index] + Constants.KEY_SEPERATOR + tokens[LogRecordToken.SequenceID.Index];

			result.startTime = Long.parseLong(tokens[LogRecordToken.StartTime.Index+added]);
			result.endTime = Long.parseLong(tokens[LogRecordToken.EndTime.Index+added]);
			result.entities = getEntities(tokens[LogRecordToken.Entities.Index+added]);
		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println(line);
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			System.exit(0);
		}
		return result;
	}

	private static Entity[] getEntities(String entitiesString) {
		// CUSTOMER 1-3-154 [BALANCE:-10.0:R#YTD_P:10.0:R#P_CNT:1:R#LAST_O_ID:2564:R]
		String[] allEntityStrings = entitiesString.split(Constants.ENTITY_SEPERATOR_REGEX);
		Entity[] result = new Entity[allEntityStrings.length];
		for (int i = 0; i < allEntityStrings.length; i++) {
			String[] entityInfo = allEntityStrings[i].split(Constants.ENTITY_ATTRIBUTE_SEPERATOR_REGEX);
			String[] allProperties = entityInfo[EntityToken.Property.Index].split(Constants.PROPERY_SEPERATOR_REGEX);

			Property[] properties = new Property[allProperties.length];
			for (int j = 0; j < allProperties.length; j++) {
				String[] propertyInfo = allProperties[j].split(Constants.PROPERY_ATTRIBUTE_SEPERATOR_REGEX);
				properties[j] = new Property(propertyInfo[PropertyToken.Name.Index], propertyInfo[PropertyToken.Value.Index], propertyInfo[PropertyToken.Type.Index].charAt(0));
			}
			result[i] = new Entity(entityInfo[EntityToken.Key.Index], entityInfo[EntityToken.Name.Index], properties);
		}
		return result;
	}

	public int getNumOfOccurrences(String[] tokens, char delimiter, int startFrom) {
		int count = 0;
		for (int i = startFrom; i < tokens.length; i++)
			if (tokens[i].charAt(0) == delimiter)
				count++;
		return count;
	}

	public boolean overlap(LogRecord i) {
		if (startTime <= i.startTime && endTime >= i.endTime)
			return true;
		if (startTime >= i.startTime && endTime <= i.endTime)
			return true;
		if (startTime >= i.startTime && startTime <= i.endTime)
			return true;
		if (endTime >= i.startTime && endTime <= i.endTime)
			return true;
		return false;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public char getType() {
		return type;
	}

	public void setType(char type) {
		this.type = type;
	}

	public Entity[] getEntities() {
		return entities;
	}

	public void setEntities(Entity[] entities) {
		this.entities = entities;
	}

	public String getActionName() {
		return actionName;
	}

	public void setActionName(String actionName) {
		this.actionName = actionName;
	}

	public boolean intersect(LogRecord log) {
		for (int i = 0; i < entities.length; i++)
			for (int j = 0; j < log.entities.length; j++) {
				if (entities[i].getName().equals(log.entities[j].getName()) && entities[i].getKey().equals(log.entities[j].getKey())) {
					return areRefSameProperties(entities[i], log.entities[j]);
				}
			}
		return false;
	}

	// TODO make sure if this checks the same pointer or not
	private boolean areRefSameProperties(Entity entity1, Entity entity2) {
		for (int i = 0; i < entity1.getProperties().length; i++) {
			for (int j = 0; j < entity2.getProperties().length; j++) {
				if (entity1.getProperties()[i] == null || entity2.getProperties()[j] == null)
					continue;
				if (entity1.getProperties()[i].getName().equals(entity2.getProperties()[j].getName())) {
					if ((entity1.getProperties()[i].getType() != Constants.NO_READ_UPDATE) && (entity2.getProperties()[j].getType() != Constants.NO_READ_UPDATE))
						return true;
				}
			}
		}
		return false;
	}

	public boolean intersect(long et) {
		return (startTime <= et && et <= endTime ? true : false);
	}

	public String getPartitionID() {
		return partitionID;
	}

	public void setPartitionID(String partitionID) {
		this.partitionID = partitionID;
	}
	public static String escapeCharacters(String input) {
		input = input.replaceAll(String.valueOf(Constants.ESCAPE_START_CHAR), Constants.ESCAPE_START_CHAR + Constants.ESCAPE_START_NUM + Constants.ESCAPE_END_CHAR);
		input = input.replaceAll("(?<!\\(\\d\\d?)" + String.valueOf(Constants.ESCAPE_END_CHAR), Constants.ESCAPE_START_CHAR + Constants.ESCAPE_END_NUM + Constants.ESCAPE_END_CHAR);
		input = input.replaceAll(String.valueOf(Constants.KEY_SEPERATOR), Constants.ESCAPE_START_CHAR + Constants.KEY_SEPERATOR_NUM + Constants.ESCAPE_END_CHAR);
		input = input.replaceAll(String.valueOf(Constants.RECORD_ATTRIBUTE_SEPERATOR), Constants.ESCAPE_START_CHAR + Constants.RECORD_ATTRIBUTE_SEPERATOR_NUM + Constants.ESCAPE_END_CHAR);
		input = input.replaceAll(String.valueOf(Constants.ENTITY_SEPERATOR), Constants.ESCAPE_START_CHAR + Constants.ENTITY_SEPERATOR_NUM + Constants.ESCAPE_END_CHAR);
		input = input.replaceAll(String.valueOf(Constants.PROPERY_SEPERATOR), Constants.ESCAPE_START_CHAR + Constants.PROPERY_SEPERATOR_NUM + Constants.ESCAPE_END_CHAR);
		input = input.replaceAll(String.valueOf(Constants.PROPERY_ATTRIBUTE_SEPERATOR), Constants.ESCAPE_START_CHAR + Constants.PROPERY_ATTRIBUTE_SEPERATOR_NUM + Constants.ESCAPE_END_CHAR);
		input = input.replaceAll(String.valueOf(Constants.ENTITY_ATTRIBUTE_SEPERATOR), Constants.ESCAPE_START_CHAR + Constants.ENTITY_ATTRIBUTE_SEPERATOR_NUM + Constants.ESCAPE_END_CHAR);

		return input;
	}
	public static class Comparators {

		public static Comparator<LogRecord> START = new Comparator<LogRecord>() {

			@Override
			public int compare(LogRecord o1, LogRecord o2) {
				return (o1.startTime > o2.startTime ? 1 : (o1.startTime < o2.startTime ? -1 : 0));
			}
		};
		public static Comparator<LogRecord> END = new Comparator<LogRecord>() {
			@Override
			public int compare(LogRecord o1, LogRecord o2) {
				return (o1.endTime > o2.endTime ? 1 : (o1.endTime < o2.endTime ? -1 : 0));
			}
		};
	}

	@Override
	public String toString() {
		String result = "";
		if (false) {
			String newActionName = actionName;
			// switch (actionName) {
			// case "GetProfile":
			// newActionName = "GP";
			// break;
			// case "GetFriends":
			// newActionName = "GF";
			// break;
			// case "GetPendingFriends":
			// newActionName = "GPF";
			// break;
			// case "InviteFriends":
			// newActionName = "IF";
			// break;
			// case "RejectFriend":
			// newActionName = "RF";
			// break;
			// case "AcceptFriend":
			// newActionName = "AF";
			// break;
			// case "Unfriendfriend":
			// newActionName = "UF";
			// break;
			// }
			result = String.valueOf(type) + ',' + newActionName + ",\t" + id + ",\t" + startTime + ",\t" + endTime + ",\t";
			if (entities != null) {
				for (int i = 0; i < entities.length; i++) {
					result += getEntityString(entities[i]);
				}
			}
		} else {
			result = id + "(" + startTime + ", " + endTime + ")";
		}
		return result;
	}

	private String getEntityString(Entity entity) {
		StringBuilder sb = new StringBuilder();
		sb.append(entity.getName());
		sb.append(Constants.ENTITY_ATTRIBUTE_SEPERATOR);
		sb.append(entity.getKey());
		sb.append(Constants.ENTITY_ATTRIBUTE_SEPERATOR);
		sb.append(getProperitiesString(entity.getProperties()));
		return sb.toString();
	}

	private String getProperitiesString(Property[] properties) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < properties.length; i++) {
			sb.append(properties[i].getName());
			sb.append(Constants.PROPERY_ATTRIBUTE_SEPERATOR);
			sb.append(properties[i].getValue());
			sb.append(Constants.PROPERY_ATTRIBUTE_SEPERATOR);
			sb.append(properties[i].getType());
			if ((i + 1) != properties.length)
				sb.append(Constants.PROPERY_SEPERATOR);
		}
		return sb.toString();
	}

	@Override
	public int compareTo(LogRecord o) {
		// String st1 = startTime + id;
		// String st2 = o.startTime + o.id;
		return Long.compare(startTime, o.startTime);
		// if (startTime > o.startTime)
		// return 1;
		// else if (startTime < o.startTime)
		// return -1;
		// else if (id.compareTo(o.id) == 0)
		// return 0;
		// else
		// return -1;
		// return id.compareTo(o.id);
		// return (startTime > o.startTime ? 1 : (startTime < o.startTime ? -1 : (id.compareTo(o.id) ? 0 : -1)));
	}

}
