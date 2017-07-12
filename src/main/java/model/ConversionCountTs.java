package model;


import java.io.Serializable;
import java.util.Date;

/**
 * Holds the count and last timestamp for a conversion pixel
 * @author heng
 */

public class ConversionCountTs implements Serializable {
  
	private Integer count;
	private Date lastConversionTs;

	public ConversionCountTs(){}
	
	public ConversionCountTs(Integer count, Date lastConversionTs ) {
		this.count = count;
		this.lastConversionTs = lastConversionTs;
	}

  public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	public Date getLastConversionTs() {
		return lastConversionTs;
	}

	public void setLastConversionTs(Date lastConversionTs) {
		this.lastConversionTs = lastConversionTs;
	}

	@Override
	public String toString() {
		return "ConversionData[" + count + "," + lastConversionTs + "]";
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((count == null) ? 0 : count.hashCode());
		result = prime * result
				+ ((lastConversionTs == null) ? 0 : lastConversionTs.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		ConversionCountTs other = (ConversionCountTs) obj;
		if (count == null) {
			if (other.count != null) {
				return false;
			}
		} else if (!count.equals(other.count)) {
			return false;
		}
		if (lastConversionTs == null) {
			if (other.lastConversionTs != null) {
				return false;
			}
		} else if (!lastConversionTs.equals(other.lastConversionTs)) {
			return false;
		}
		return true;
	}

}

