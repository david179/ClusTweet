package it.unipd.dei.db;

/**
* Custom class to throw an exception that manages the situation when 
* for each subset Pj the number of centers is greater than the number of elements in the subset
*/

@SuppressWarnings("serial")
public class KcoeffCustomException extends Exception
{
	public KcoeffCustomException(String message)
	{
		super(message);
	}
}
