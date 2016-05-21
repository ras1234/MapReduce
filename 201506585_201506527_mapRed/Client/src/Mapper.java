public class Mapper implements IMapper{
	
	@Override
	public String map(String text) {
		String out="";
		String SearchKey = MapTask.searchKey;
		if(text.contains(SearchKey))
			return out;
		else
			return null;
	}

}