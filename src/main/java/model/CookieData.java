package model;


import gnu.trove.map.hash.TIntShortHashMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 
 * @author mcoronado
 * 
 * 
 *         version 1 - Initial CookieData version 
 *         version 2 - Added linkedCookies object
 *         version 3 - Added monthly/lifetime pkg/io impressions trove maps 
 */
// whenever this class changes the version must be incremented
public class CookieData implements Serializable {
	// define this so it isn't env dependent
	private static final long serialVersionUID = 1L;

	private long cookieId;

	private long lastUpdate;
	private boolean isOptOut = false;
	private RtmValue rtmValue;
	private Collection<Integer> segments = new ArrayList<Integer>();
	private Map<Integer, KeyValueTs> keyValues = new HashMap<Integer, KeyValueTs>();
	private Map<Integer, RtmValue> rtmValues = new HashMap<Integer, RtmValue>();
	private TIntShortHashMap monthlyCampaignImpressions = null;
	private TIntShortHashMap monthlyLineItemImpressions = null;
	private TIntShortHashMap monthlyInsertionOrderImpressions = null;
	private TIntShortHashMap lifetimeLineItemImpressions = null;
	private TIntShortHashMap lifetimeInsertionOrderImpressions = null;

	private Map<Integer, ConversionCountTs> conversionCounts = new HashMap<Integer, ConversionCountTs>();
	/** linked cookies are for cross devices */
	private Map<Long, CookieDataNonEntity> linkedCookies = new HashMap<Long, CookieDataNonEntity>();

	public final static int MAX_TIER_VALUE = 9999;
	public final static int LOCAL_TIER_VALUE = 1;

	public CookieData(long cookeId) {
		this.cookieId = cookeId;
	}

	public CookieData() {
	}

	public long getCookieId() {
		return cookieId;
	}

	public void setCookieId(long cookieId) {
		this.cookieId = cookieId;
	}

	public Map<Long, CookieDataNonEntity> getLinkedCookies() {
		return linkedCookies;
	}

	public CookieDataNonEntity getLinkedCookieNonEntity(long childCookieId) {
		return linkedCookies.get(childCookieId);
	}

	public void addLinkedCookie(CookieDataNonEntity linkedCookie) {
		if (linkedCookie != null) {
			this.linkedCookies.put(linkedCookie.getCookieId(), linkedCookie);
			setLastUpdate(linkedCookie.getLastUpdate());
		}
	}

	/**
	 * remove linkedCookie to current cookie
	 */
	public void removeLinkedCookie(Long id) throws Exception {
		this.linkedCookies.remove(id);

		// Bug 13938: don't set lastUpdate to now().
		// setLastUpdate(new Date()); // do we need this? -- remove it for now;
		// will review this logic again. 01/2015
	}

	public void clearLinkedCookie() {
		this.linkedCookies = new HashMap<Long, CookieDataNonEntity>();

		// Bug 13938: don't set lastUpdate to now().
		// setLastUpdate(new Date()); // do we need this? -- remove it for now;
		// will review this logic again. 01/2015
	}

	public RtmValue getRtmValue() {
		return rtmValue;
	}

	/**
	 * get the rtm value that's the news of the linked cookies
	 */
	public RtmValue getRtmValueLinked(int tier) {
		RtmValue retRtmValue = getRtmValue();
		for (Long cid : linkedCookies.keySet()) {
			CookieDataNonEntity child = linkedCookies.get(cid);
			if (child != null && child.getTier()<=tier) {
				RtmValue childRtmValue = child.getRtmValue();

				if (childRtmValue != null) {
					// if merging value does not exist in the current or is
					// older then
					// replace
					if (retRtmValue == null
							|| retRtmValue
									.getLastImpressionTsSecondsFromEpoch() == null
							|| (childRtmValue
									.getLastImpressionTsSecondsFromEpoch() != null && childRtmValue
									.getLastImpressionTs().after(
											retRtmValue.getLastImpressionTs()))) {
						retRtmValue = childRtmValue;
					}
				}
			}
		}
		return retRtmValue;
	}

	public void setRtmValue(RtmValue rtmValue) {
		if (rtmValue == null) {
			// Bug 13938: don't set lastUpdate to now().
			// setLastUpdate(new Date());
		} else {
			setLastUpdate(rtmValue.getLastImpressionTs());
		}
		this.rtmValue = rtmValue;
	}

	public boolean isOptOut() {
		return isOptOut;
	}

	/**
	 * the cookie is opted out if 1 of the linked cookie is opted out
	 */
	public boolean isOptOutLinked(int tier) {
		boolean opt = this.isOptOut();
		for (Long cid : linkedCookies.keySet()) {
			CookieDataNonEntity child = linkedCookies.get(cid);
			if (child != null && child.getTier() <= tier) {
				opt |= child.isOptOut();
			}
		}
		return opt;
	}

	public void setOptOut(boolean isOptOut) {
		this.isOptOut = isOptOut;
	}

	public Collection<Integer> getSegments() {
		return segments;
	}

	/**
	 * collect all segments of the linked cookies
	 * 
	 * @return
	 */
	public Collection<Integer> getSegmentsLinked(int tier) {
		Collection<Integer> seg = getSegments();
		for (Long cid : linkedCookies.keySet()) {
			CookieDataNonEntity child = linkedCookies.get(cid);
			if (child != null && child.getTier()<=tier) {
				seg.addAll(child.getSegments());
			}
		}
		return seg;
	}

	public void setSegments(Collection<Integer> segments) {
		if (segments != null) {
			this.segments = segments;
		}
	}

	public Map<Integer, KeyValueTs> getKeyValues() {
		return keyValues;
	}

	/**
	 * collapse the cookie list based on merge results, which give priority to
	 * newest time
	 */
	public Map<Integer, KeyValueTs> getKeyValuesLinked(int tier) {
		Map<Integer, KeyValueTs> retKV = new HashMap<Integer, KeyValueTs>();
		retKV.putAll(getKeyValues());
		for (Long cid : linkedCookies.keySet()) {
			CookieDataNonEntity child = linkedCookies.get(cid);
			if (child != null && child.getTier()<=tier) {
				Map<Integer, KeyValueTs> childKV = child.getKeyValues();
				if (childKV != null) {
					for (Integer key : childKV.keySet()) {
						KeyValueTs merge = childKV.get(key);
						KeyValueTs kvtInCache = retKV.get(key);
						if (kvtInCache == null
								|| kvtInCache.getLastPixelTs() == null
								|| (merge.getLastPixelTs() != null && merge
										.getLastPixelTs().after(
												kvtInCache.getLastPixelTs()))) {
							// replace the key
							retKV.put(key, merge);
							setLastUpdate(merge.getLastPixelTs());
						}

					}
				}
			}
		}
		return retKV;
	}

	public void setKeyValues(Map<Integer, KeyValueTs> keyValues) {
		for (KeyValueTs kvt : keyValues.values()) {
			setLastUpdate(kvt.getLastPixelTs());
		}
		this.keyValues = keyValues;
	}

	public void mergeKeyValues(Map<Integer, KeyValueTs> keys) {
		if (keys != null) {
			for (Integer key : keys.keySet()) {
				KeyValueTs merge = keys.get(key);
				KeyValueTs kvtInCache = keyValues.get(key);
				if (kvtInCache == null
						|| kvtInCache.getLastPixelTs() == null
						|| (merge.getLastPixelTs() != null && merge
								.getLastPixelTs().after(
										kvtInCache.getLastPixelTs()))) {
					// replace the key
					keyValues.put(key, merge);
					setLastUpdate(merge.getLastPixelTs());
				}
			}
		}
	}

	//Cosmo1026-Joe: CookieDataLogic.mergeLinkedCookieData() should be the only place for merging RtmValue and RtmValues 
/*	public void mergeRtmValue(RtmValue inValue) {
	}
	
	public void mergeRtmValues(Map<Integer, RtmValue> inValues) {
	}
*/
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
								.getLastConversionTs().after(
										cachedValue.getLastConversionTs()))) {

					int oldCount = (cachedValue == null ? 0 : cachedValue
							.getCount());
					int newCount = oldCount + mergeValue.getCount();
					ConversionCountTs newValue = new ConversionCountTs(
							newCount, mergeValue.getLastConversionTs());
					conversionCounts.put(cpid, newValue);
					setLastUpdate(mergeValue.getLastConversionTs());
				}
			}
		}
	}

	public Map<Integer, RtmValue> getRtmValues() {
		return rtmValues;
	}

	/**
	 * collapse the cookie rtm values based on merge results, which give
	 * priority to newest time
	 */
	public Map<Integer, RtmValue> getRtmValuesLinked(int tier) {
		Map<Integer, RtmValue> retRv = new HashMap<Integer, RtmValue>();
		if (getRtmValues() != null)
			retRv.putAll(getRtmValues());
		for (Long cid : linkedCookies.keySet()) {
			CookieDataNonEntity child = linkedCookies.get(cid);
			if (child != null && child.getTier()<=tier) {
				Map<Integer, RtmValue> childRV = child.getRtmValues();
				if (childRV != null) {
					for (Integer cpid : childRV.keySet()) {
						RtmValue mergeRtmValue = childRV.get(cpid);
						RtmValue cachedRtmValue = retRv.get(cpid);
						// if merging value does not exist in the cache or is older then replace
						if (cachedRtmValue == null
								|| cachedRtmValue
										.getLastImpressionTsSecondsFromEpoch() == null
								|| (mergeRtmValue
										.getLastImpressionTsSecondsFromEpoch() != null && mergeRtmValue
										.getLastImpressionTs().after(
												cachedRtmValue
														.getLastImpressionTs()))) {
							retRv.put(cpid, mergeRtmValue);
							setLastUpdate(mergeRtmValue.getLastImpressionTs());
						}

					}
				}
			}
		}
		return retRv;
	}

	public void setRtmValues(Map<Integer, RtmValue> rtmValues) {
		if (rtmValues != null) {
			for (RtmValue rtm : rtmValues.values()) {
				setLastUpdate(rtm.getLastImpressionTs());
			}

			this.rtmValues = rtmValues;
		}
	}
	
	public TIntShortHashMap getMonthlyCampaignImpressions() {
		return monthlyCampaignImpressions;
	}

	public void setMonthlyCampaignImpressions(TIntShortHashMap monthlyCampaignImpressions) {
		this.monthlyCampaignImpressions = monthlyCampaignImpressions;
	}

	public TIntShortHashMap getMonthlyLineItemImpressions() {
		return monthlyLineItemImpressions;
	}

	public void setMonthlyLineItemImpressions(
			TIntShortHashMap monthlyLineItemImpressions) {
		this.monthlyLineItemImpressions = monthlyLineItemImpressions;
	}

	public TIntShortHashMap getMonthlyInsertionOrderImpressions() {
		return monthlyInsertionOrderImpressions;
	}

	public void setMonthlyInsertionOrderImpressions(TIntShortHashMap monthlyInsertionOrderImpressions) {
		this.monthlyInsertionOrderImpressions = monthlyInsertionOrderImpressions;
	}

	public TIntShortHashMap getLifetimeLineItemImpressions() {
		return lifetimeLineItemImpressions;
	}

	public void setLifetimeLineItemImpressions(
			TIntShortHashMap lifetimeLineItemImpressions) {
		this.lifetimeLineItemImpressions = lifetimeLineItemImpressions;
	}

	public TIntShortHashMap getLifetimeInsertionOrderImpressions() {
		return lifetimeInsertionOrderImpressions;
	}

	public void setLifetimeInsertionOrderImpressions(TIntShortHashMap lifetimeInsertionOrderImpressions) {
		this.lifetimeInsertionOrderImpressions = lifetimeInsertionOrderImpressions;
	}

	public Map<Integer, ConversionCountTs> getConversionCounts() {
		return conversionCounts;
	}

	public Map<Integer, ConversionCountTs> getConversionCountsLinked(int tier) {
		Map<Integer, ConversionCountTs> retConv = new HashMap<Integer, ConversionCountTs>();
		if (getConversionCounts() != null)
			retConv.putAll(getConversionCounts());
		for (Long cid : linkedCookies.keySet()) {
			CookieDataNonEntity child = linkedCookies.get(cid);
			if (child != null && child.getTier()<=tier) {
				Map<Integer, ConversionCountTs> childConv = child
						.getConversionCounts();
				if (childConv != null) {
					for (Integer cpid : childConv.keySet()) {
						ConversionCountTs mergeValue = childConv.get(cpid);
						ConversionCountTs cachedValue = retConv.get(cpid);
						// if merging value does not exist in the cache or is older then replace
						if (cachedValue == null
								|| cachedValue.getLastConversionTs() == null
								|| (mergeValue.getLastConversionTs() != null && mergeValue
										.getLastConversionTs().after(
												cachedValue
														.getLastConversionTs()))) {

							int oldCount = (cachedValue == null ? 0
									: cachedValue.getCount());
							int newCount = oldCount + mergeValue.getCount();
							ConversionCountTs newValue = new ConversionCountTs(
									newCount, mergeValue.getLastConversionTs());
							retConv.put(cpid, newValue);
							setLastUpdate(mergeValue.getLastConversionTs());
						}
					}
				}
			}

		}
		return retConv;
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

	public String toStringCurrent() {
		StringBuilder sb = new StringBuilder();
		sb.append("cookieId:").append(cookieId);
		sb.append("\n");
		sb.append("lastUpdate:").append(new Date(lastUpdate));
		sb.append("\n");
		sb.append("optOut:").append(isOptOut);
		sb.append("\n");
		sb.append("linked cookie:").append(linkedCookies.size());
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
				sb.append("rtm:").append(rtm).append(" ")
						.append(rtmValues.get(rtm)).append("\n");
			}
		}
		if(monthlyCampaignImpressions != null && monthlyCampaignImpressions.size() > 0){
			for (int campaignId : monthlyCampaignImpressions.keys()){
				sb.append("monthlyCampaignImpressions:").append(campaignId).append(" ")
				        .append(monthlyCampaignImpressions.get(campaignId)).append("\n");
			}
		}

		if(monthlyLineItemImpressions != null && monthlyLineItemImpressions.size() > 0){
			for (int lineItemId : monthlyLineItemImpressions.keys()){
				sb.append("monthlyLineItemImpressions:").append(lineItemId).append(" ")
				        .append(monthlyLineItemImpressions.get(lineItemId)).append("\n");
			}
		}

		if(monthlyInsertionOrderImpressions != null && monthlyInsertionOrderImpressions.size() > 0){
			for (int insertionOrderId : monthlyInsertionOrderImpressions.keys()){
				sb.append("monthlyInsertionOrderImpressions:").append(insertionOrderId).append(" ")
				        .append(monthlyInsertionOrderImpressions.get(insertionOrderId)).append("\n");
			}
		}

		if(lifetimeLineItemImpressions != null && lifetimeLineItemImpressions.size() > 0){
			for (int lineItemId : lifetimeLineItemImpressions.keys()){
				sb.append("lifetimeLineItemImpressions:").append(lineItemId).append(" ")
				        .append(lifetimeLineItemImpressions.get(lineItemId)).append("\n");
			}
		}

		if(lifetimeInsertionOrderImpressions != null && lifetimeInsertionOrderImpressions.size() > 0){
			for (int insertionOrderId : lifetimeInsertionOrderImpressions.keys()){
				sb.append("lifetimeInsertionOrderImpressions:").append(insertionOrderId).append(" ")
				        .append(lifetimeInsertionOrderImpressions.get(insertionOrderId)).append("\n");
			}
		}

		if (conversionCounts != null && conversionCounts.size() > 0) {
			for (Integer c : conversionCounts.keySet()) {
				sb.append("id:").append(c).append(" ")
						.append(conversionCounts.get(c)).append("\n");
			}
		}
		sb.append("size:" + getSize());
		sb.append("\n");
		return sb.toString();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(toStringCurrent());
		
		sb.append("========================\n");
		sb.append("linked cookies\n");
		sb.append("========================\n");
		
		if (linkedCookies!=null && linkedCookies.size()>0) {
			for (Long cookieId : linkedCookies.keySet()) {
				CookieDataNonEntity linkedCookie = linkedCookies.get(cookieId);
				if (linkedCookie != null) {
					sb.append("===cookieId===:");
					sb.append(cookieId);
					sb.append("\n");
					sb.append(linkedCookie.toString());
					sb.append("\n");
				}
			}
		}
		
		return sb.toString();
	}

	public String toStringCombined() {
		StringBuilder sb = new StringBuilder();
		sb.append("\n *** Cookie values:*** \n");
		sb.append("cookieId:").append(cookieId);
		sb.append("\n");
		sb.append("lastUpdate:").append(new Date(lastUpdate));
		sb.append("\n");
		sb.append("optOut:").append(isOptOutLinked(MAX_TIER_VALUE));
		sb.append("\n");
		sb.append("number of linked cookies:").append(linkedCookies.size());
		sb.append("\n");

		Collection<Integer> segs = getSegmentsLinked(MAX_TIER_VALUE);
		if (segs != null && segs.size() > 0) {
			sb.append("segments:");
			for (Integer seg : segs) {
				sb.append(seg).append(",");
			}
			sb.append("\n");
		}

		Map<Integer, KeyValueTs> kvs = getKeyValuesLinked(MAX_TIER_VALUE);
		if (kvs != null && kvs.size() > 0) {
			for (Integer kvt : kvs.keySet()) {
				sb.append(kvs.get(kvt)).append("\n");
			}
		}

		RtmValue rtmValue = getRtmValueLinked(MAX_TIER_VALUE);
		if (rtmValue != null) {
			sb.append("rtmValue:").append(rtmValue).append("\n");
		}

		Map<Integer, RtmValue> rvs = getRtmValuesLinked(MAX_TIER_VALUE);
		if (rvs != null && rvs.size() > 0) {
			for (Integer rtm : rvs.keySet()) {
				sb.append("rtm:").append(rtm).append(" ").append(rvs.get(rtm))
						.append("\n");
			}
		}

		Map<Integer, ConversionCountTs> ccs = getConversionCountsLinked(MAX_TIER_VALUE);
		if (ccs != null && ccs.size() > 0) {
			for (Integer c : ccs.keySet()) {
				sb.append("id:").append(c).append(" ").append(ccs.get(c))
						.append("\n");
			}
		}

		// print out the cookie id instead of cookie data
		Set<Long> lcs = getLinkedCookieIds();
		if (lcs != null && lcs.size() > 0) {
			for (Long c : lcs) {
				sb.append("linked cookie:").append(c).append("\n");
			}
		}

		sb.append("size:" + getSize());
		sb.append("\n");

		// Add current cookie values:
		if (linkedCookies.size() > 0) {
			sb.append(" *** Current cookie values:*** \n");
			sb.append(this.toStringCurrent());
		}

		return sb.toString();
	}

	public int getSize() {
		int totalSize = 9; // default size (2 longs + boolean)

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

		if (getLinkedCookies() != null) {
			totalSize += 16;
			Map<Long, CookieDataNonEntity> lcs = getLinkedCookies();
			if (lcs != null && lcs.size() > 0) {
				for (Long c : lcs.keySet()) {
					CookieDataNonEntity cd = lcs.get(c);
					if (cd != null)
						totalSize += cd.getSize();
				}
			}
		}

		return totalSize;
	}

	public Set<Long> getLinkedCookieIds() {
		// wrap it in a HashSet so that it's serializable
		return new HashSet<Long>(this.getLinkedCookies().keySet());
	}
	
	public static TIntShortHashMap createDefaultTIntShortHashMap(){
		return new TIntShortHashMap(4, 0.75f);
	}

}
