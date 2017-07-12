package model;



import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 *         version 1 - Inital CookieData version 2 - Added linkedCookies object
 */
	public class CookieDataNonEntity implements Serializable {
		// define this so it isn't env dependent
		private static final long serialVersionUID = 1L;

		private long cookieId;

		private long lastUpdate;
		private boolean isOptOut = false;
		private RtmValue rtmValue;
		private Collection<Integer> segments = new ArrayList<Integer>();
		private Map<Integer, KeyValueTs> keyValues = new HashMap<Integer, KeyValueTs>();
		private Map<Integer, RtmValue> rtmValues = new HashMap<Integer, RtmValue>();
		private Map<Integer, ConversionCountTs> conversionCounts = new HashMap<Integer, ConversionCountTs>();
		
		// default link is tier 3
		private int tier = 3;

		public CookieDataNonEntity(long cookeId) {
			this.cookieId = cookeId;
		}

		public CookieDataNonEntity() {
		}
		
		public long getCookieId() {
			return cookieId;
		}

		public void setCookieId(long cookieId) {
			this.cookieId = cookieId;
		}

		public RtmValue getRtmValue() {
			return rtmValue;
		}

		public void setRtmValue(RtmValue rtmValue) {
			if (rtmValue == null) {
		    	//Bug 13938: don't set lastUpdate to now().
				// setLastUpdate(new Date());
			} else {
				setLastUpdate(rtmValue.getLastImpressionTs());
			}
			this.rtmValue = rtmValue;
		}

		public boolean isOptOut() {
			return isOptOut;
		}

		public void setOptOut(boolean isOptOut) {
			this.isOptOut = isOptOut;
		}

		public Collection<Integer> getSegments() {
			return segments;
		}


		public void setSegments(Collection<Integer> segments) {
			if (segments != null) {
				this.segments = segments;
			}
		}

		public Map<Integer, KeyValueTs> getKeyValues() {
			return keyValues;
		}

		public void setKeyValues(Map<Integer, KeyValueTs> keyValues) {
			for (KeyValueTs kvt : keyValues.values()) {
				setLastUpdate(kvt.getLastPixelTs());
			}
			this.keyValues = keyValues;
		}

		public void mergeKeyValues(Map<Integer, KeyValueTs> keys) {
			if (keyValues == null)
				keyValues = new HashMap<Integer, KeyValueTs>();
			if (keys != null) {
				for (Integer key : keys.keySet()) {
					KeyValueTs merge = keys.get(key);
					KeyValueTs kvtInCache = keyValues.get(key);
					if (kvtInCache == null
							|| kvtInCache.getLastPixelTs() == null
							|| (merge.getLastPixelTs() != null && merge.getLastPixelTs().after(
									kvtInCache.getLastPixelTs()))) {
						// replace the key
						keyValues.put(key, merge);
						setLastUpdate(merge.getLastPixelTs());
					}
				}
			}
		}

		public void mergeRtmValue(RtmValue inValue) {
			if (inValue != null) {
				// if merging value does not exist in the cache or is older then replace
				if (rtmValue == null
						|| rtmValue.getLastImpressionTsSecondsFromEpoch() == null
						|| (inValue.getLastImpressionTsSecondsFromEpoch() != null && inValue
								.getLastImpressionTs().after(rtmValue.getLastImpressionTs()))) {
					rtmValue = inValue;
					setLastUpdate(inValue.getLastImpressionTs());
				}
			}
		}

		public void mergeRtmValues(Map<Integer, RtmValue> inValues) {
			if (inValues != null) {
				if (rtmValues == null) {
					rtmValues = new HashMap<Integer, RtmValue>();
				}
				for (Integer cpid : inValues.keySet()) {
					RtmValue mergeRtmValue = inValues.get(cpid);
					RtmValue cachedRtmValue = rtmValues.get(cpid);
					// if merging value does not exist in the cache or is older then replace
					if (cachedRtmValue == null
							|| cachedRtmValue.getLastImpressionTsSecondsFromEpoch() == null
							|| (mergeRtmValue.getLastImpressionTsSecondsFromEpoch() != null && mergeRtmValue
									.getLastImpressionTs().after(
											cachedRtmValue.getLastImpressionTs()))) {
						rtmValues.put(cpid, mergeRtmValue);
						setLastUpdate(mergeRtmValue.getLastImpressionTs());
					}
				}
			}
		}

		public void mergeConversion(Map<Integer, ConversionCountTs> inCnv) {
			if (inCnv != null) {
				if (conversionCounts == null) {
					conversionCounts = new HashMap<Integer, ConversionCountTs>();
				}
				for (Integer cpid : inCnv.keySet()) {
					ConversionCountTs mergeValue = inCnv.get(cpid);
					ConversionCountTs cachedValue = conversionCounts.get(cpid);
					// if merging value does not exist in the cache or is older then replace
					if (cachedValue == null
							|| cachedValue.getLastConversionTs() == null
							|| (mergeValue.getLastConversionTs() != null && mergeValue
									.getLastConversionTs().after(cachedValue.getLastConversionTs()))) {

						int oldCount = (cachedValue == null ? 0 : cachedValue.getCount());
						int newCount = oldCount + mergeValue.getCount();
						ConversionCountTs newValue = new ConversionCountTs(newCount,
								mergeValue.getLastConversionTs());
						conversionCounts.put(cpid, newValue);
						setLastUpdate(mergeValue.getLastConversionTs());
					}
				}
			}
		}

		public Map<Integer, RtmValue> getRtmValues() {
			return rtmValues;
		}


		public void setRtmValues(Map<Integer, RtmValue> rtmValues) {
			if (rtmValues != null) {
				for (RtmValue rtm : rtmValues.values()) {
					setLastUpdate(rtm.getLastImpressionTs());
				}

				this.rtmValues = rtmValues;
			}
		}

		public Map<Integer, ConversionCountTs> getConversionCounts() {
			return conversionCounts;
		}

		public void setConversionCounts(
				Map<Integer, ConversionCountTs> conversionCounts) {
			if (conversionCounts != null) {
				for (ConversionCountTs cts : conversionCounts.values()) {
					setLastUpdate(cts.getLastConversionTs());
				}
				this.conversionCounts = conversionCounts;
			}
		}

		public void setLastUpdate(Date timestamp) {
			if (timestamp != null && timestamp.getTime() > lastUpdate) {
				lastUpdate = timestamp.getTime();
			}
		}

		public Date getLastUpdate() {
			return new Date(lastUpdate);
		}

		public int getTier() {
			return tier;
		}

		public void setTier(int tier) {
			this.tier = tier;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("cookieId:").append(cookieId);
			sb.append("\n");
			sb.append("lastUpdate:").append(new Date(lastUpdate));
			sb.append("\n");
			sb.append("optOut:").append(isOptOut);
			sb.append("\n");
			sb.append("tier:").append(tier);
			sb.append("\n");
			if (segments != null && segments.size() > 0) {
				sb.append("segments:");
				for (Integer seg : segments) {
					sb.append(seg).append(",");
				}
				sb.append("\n");
			}

			if (keyValues != null && keyValues.size() > 0) {
				for (Integer kvt : keyValues.keySet()) {
					sb.append(keyValues.get(kvt)).append("\n");
				}
			}

			if (rtmValue != null) {
				sb.append("rtmValue:").append(rtmValue).append("\n");
			}

			if (rtmValues != null && rtmValues.size() > 0) {
				for (Integer rtm : rtmValues.keySet()) {
					sb.append("rtm:").append(rtm).append(" ").append(rtmValues.get(rtm))
							.append("\n");
				}

			}

			if (conversionCounts != null && conversionCounts.size() > 0) {
				for (Integer c : conversionCounts.keySet()) {
					sb.append("id:").append(c).append(" ").append(conversionCounts.get(c))
							.append("\n");
				}
			}
			sb.append("size:" + getSize());
			sb.append("\n");
			return sb.toString();
		}

		public int getSize() {
			int totalSize = 13; // default size (2 longs + 1 int + boolean)

			// segments
			if (segments != null) {
				totalSize += segments.size() * 4;
			}

			// key values
			if (keyValues != null) {
				for (Integer key : keyValues.keySet()) {
					totalSize += ((KeyValueTs) keyValues.get(key)).getSize();
				}
			}

			// rtm values
			if (rtmValues != null) {
				totalSize += rtmValues.size() * 16;
			}

			// conversions
			if (conversionCounts != null) {
				totalSize += conversionCounts.size() * 12;
			}

			return totalSize;
		}

	}

