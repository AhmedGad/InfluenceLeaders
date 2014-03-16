package DataSummary;

import java.io.Serializable;
import java.util.Date;

public class UserData implements Serializable {
	public long id;
	public int followers;
	public int friends;
	public Date dateJoined;

	public UserData(long id, int followers, int friends, Date date) {
		this.followers = followers;
		this.friends = friends;
		this.dateJoined = date;
		this.id = id;
	}

}
