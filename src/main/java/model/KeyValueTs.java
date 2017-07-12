package model;


import java.io.Serializable;
import java.util.Date;

//public class KeyValueTs implements Externalizable{
public class KeyValueTs implements Serializable {
  
	private Integer keyId;
  private String value;      // value
  private Date lastPixelTs;  // last_pixel_ts

  public KeyValueTs() {}     // default constructor for serialization  
  
	public KeyValueTs(Integer keyId, String value, Date lastPixelTs ) {
		this.keyId = keyId;
		this.value = value;
		this.lastPixelTs = lastPixelTs;
	}

  public Integer getKeyId() {
		return keyId;
	}
	public void setKeyId(Integer keyId) {
		this.keyId = keyId;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public Date getLastPixelTs() {
		return lastPixelTs;
	}
	public void setLastPixelTs(Date lastPixelTs) {
		this.lastPixelTs = lastPixelTs;
	}

	public int getSize(){
    return 4 + (value == null ? 0 : value.length()) + 8;    
  }
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;	

		KeyValueTs other = (KeyValueTs) obj;
		if( other != null ){
			return false;
		}else{
			return false;
		}
	}
	
	@Override
	public String toString() {
		return "KeyValueTs[" + keyId + "," + value + "," + lastPixelTs + "]";
	}

//  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
//    keyId = in.readInt();
//    lastPixelTs = new Date(in.readLong());
//    value = in.readUTF();
//  }
//
//  public void writeExternal(ObjectOutput out) throws IOException {
//    out.writeInt (keyId);
//    out.writeLong(lastPixelTs.getTime());
//    out.writeUTF (value);
//  }
	
}

