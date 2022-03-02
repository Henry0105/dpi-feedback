package com.mob.dpi.pojo;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableStringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;

/**
 * Created by xcl on 2019/7/31 15:34.
 */
public class JavaStringObjectInspector extends
		AbstractPrimitiveJavaObjectInspector implements
		SettableStringObjectInspector {

	public JavaStringObjectInspector() {
		super(TypeInfoFactory.stringTypeInfo);
	}

	@Override
	public Text getPrimitiveWritableObject(Object o) {
		return o == null ? null : new Text(o.toString());
	}

	@Override
	public String getPrimitiveJavaObject(Object o) {
		return o == null ? null : o.toString();
	}

	@Override
	public Object create(Text value) {
		return value == null ? null : value.toString();
	}

	@Override
	public Object set(Object o, Text value) {
		return value == null ? null : value.toString();
	}

	@Override
	public Object create(String value) {
		return value;
	}

	@Override
	public Object set(Object o, String value) {
		return value;
	}
}
