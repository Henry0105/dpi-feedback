package com.mob.dpi.pojo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * ListStructObjectInspector works on struct data that is stored as a Java List
 * or Java Array object. Basically, the fields are stored sequentially in the
 * List object.
 *
 * The names of the struct fields and the internal structure of the struct
 * fields are specified in the ctor of the StructObjectInspector.
 *
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects,
 * instead of directly creating an instance of this class.
 *
 *
 * 在源码的基础上修改权限，传递字段信息
 *
 */
public class StandardStructObjectInspector extends  SettableStructObjectInspector {
  public static final Log LOG = LogFactory.getLog(org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector.class.getName());

  protected static class MyField implements StructField {
    protected int fieldID;
    protected String fieldName;
    protected ObjectInspector fieldObjectInspector;
    protected String fieldComment;

    protected MyField() {
      super();
    }
    
    public MyField(int fieldID, String fieldName,ObjectInspector fieldObjectInspector) {
      this.fieldID = fieldID;
      this.fieldName = fieldName.toLowerCase();
      this.fieldObjectInspector = fieldObjectInspector;
    }

    public MyField(int fieldID, String fieldName,ObjectInspector fieldObjectInspector, String fieldComment) {
      this(fieldID, fieldName, fieldObjectInspector);
      this.fieldComment = fieldComment;
    }

    @Override
    public int getFieldID() {
      return fieldID;
    }

    @Override
    public String getFieldName() {
      return fieldName;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return fieldObjectInspector;
    }

    @Override
    public String getFieldComment() {
      return fieldComment;
    }

    @Override
    public String toString() {
      return "" + fieldID + ":" + fieldName;
    }
  }

  protected List<MyField> fields;

  protected StandardStructObjectInspector() {
    super();
  }
  /**
   * Call ObjectInspectorFactory.getStandardListObjectInspector instead.
   */
  public StandardStructObjectInspector(List<String> structFieldNames,List<ObjectInspector> structFieldObjectInspectors) {
    init(structFieldNames, structFieldObjectInspectors, null);
  }

  /**
  * Call ObjectInspectorFactory.getStandardListObjectInspector instead.
  */
  protected StandardStructObjectInspector(List<String> structFieldNames,
                                          List<ObjectInspector> structFieldObjectInspectors,
                                          List<String> structFieldComments) {
    init(structFieldNames, structFieldObjectInspectors, structFieldComments);
  }

  protected void init(List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors,
      List<String> structFieldComments) {

    fields = new ArrayList<MyField>(structFieldNames.size());
    for (int i = 0; i < structFieldNames.size(); i++) {
      fields.add(new MyField(i, structFieldNames.get(i),
          structFieldObjectInspectors.get(i),
          structFieldComments == null ? null : structFieldComments.get(i)));
    }
  }

  protected StandardStructObjectInspector(List<StructField> fields) {
    init(fields);
  }

  protected void init(List<StructField> fields) {
    this.fields = new ArrayList<MyField>(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      this.fields.add(new MyField(i, fields.get(i).getFieldName(), fields
          .get(i).getFieldObjectInspector()));
    }
  }

  @Override
  public String getTypeName() {
    return ObjectInspectorUtils.getStandardStructTypeName(this);
  }

  @Override
  public final Category getCategory() {
    return Category.STRUCT;
  }

  // Without Data
  @Override
  public StructField getStructFieldRef(String fieldName) {
    return ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields);
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return fields;
  }

  boolean warned = false;

  // With Data
  @Override
  @SuppressWarnings("unchecked")
  public Object getStructFieldData(Object data, StructField fieldRef) {
    if (data == null) {
      return null;
    }
    // We support both List<Object> and Object[]
    // so we have to do differently.
    boolean isArray = ! (data instanceof List);
    if (!isArray && !(data instanceof List)) {
      return data;
    }
    int listSize = (isArray ? ((Object[]) data).length : ((List<Object>) data).size());
    MyField f = (MyField) fieldRef;
    if (fields.size() != listSize && !warned) {
      // TODO: remove this
      warned = true;
      LOG.warn("Trying to access " + fields.size()
          + " fields inside a list of " + listSize + " elements: "
          + (isArray ? Arrays.asList((Object[]) data) : (List<Object>) data));
      LOG.warn("ignoring similar errors.");
    }
    int fieldID = f.getFieldID();

    if (fieldID >= listSize) {
      return null;
    } else if (isArray) {
      return ((Object[]) data)[fieldID];
    } else {
      return ((List<Object>) data).get(fieldID);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }
    // We support both List<Object> and Object[]
    // so we have to do differently.
    if (! (data instanceof List)) {
      data = Arrays.asList((Object[]) data);
    }
    List<Object> list = (List<Object>) data;
    return list;
  }

  // /////////////////////////////
  // SettableStructObjectInspector
  @Override
  public Object create() {
    ArrayList<Object> a = new ArrayList<Object>(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      a.add(null);
    }
    return a;
  }

  @Override
  public Object setStructFieldData(Object struct, StructField field,
      Object fieldValue) {
    ArrayList<Object> a = (ArrayList<Object>) struct;
    MyField myField = (MyField) field;
    a.set(myField.fieldID, fieldValue);
    return a;
  }

}
