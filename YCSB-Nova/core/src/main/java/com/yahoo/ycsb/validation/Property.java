package com.yahoo.ycsb.validation;
public class Property {
	private String name;
	private String value;
	private char type = Constants.NO_READ_UPDATE;

	public Property(String name, String value, char type) {
		this.name = name;
		this.value = LogRecord.escapeCharacters(value);
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public char getType() {
		return type;
	}

	public void setType(char type) {
		this.type = type;
	}

	public Property getCopy() {
		Property p = new Property(name, value, type);
		return p;
	}

	public static String getProprtyKey(Entity e, Property p) {
		StringBuilder sb = new StringBuilder();
		sb.append(e.getName());
		sb.append(Constants.KEY_SEPERATOR);
		sb.append(e.getKey());
		sb.append(Constants.KEY_SEPERATOR);
		sb.append(p.getName());
		return sb.toString();
	}
}

