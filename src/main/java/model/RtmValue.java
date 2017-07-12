package model;

import java.io.Serializable;
import java.util.Date;

public interface RtmValue extends Serializable {

  public void addClicks(int clicks);

  public void addImpressions(int impressions);

  public int getImpressions();

  public void setImpressions(int impressions);

  public int getClicks();

  public void setClicks(int clicks);

  public Integer getLastImpressionTsSecondsFromEpoch();

  public void setLastImpressionTsSecondsFromEpoch(Integer lastImpressionTs);

  public Integer getLastClickTsSecondsFromEpoch();

  public void setLastClickTsSecondsFromEpoch(Integer lastClickTs);

  public Date getLastClickTs();

  public Date getLastImpressionTs();
}
