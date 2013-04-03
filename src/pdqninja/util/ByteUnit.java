package pdqninja.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
*
* @author cb
*/
public enum ByteUnit {
   KB (1024L),
   MB (1024L * KB.value),
   GB (1024L * MB.value),
   K (KB.value),
   M (MB.value),
   G (GB.value),
   B (1L);

   private final long value;

   ByteUnit(long val) {
       this.value = val;
   }
   
   public long value() {
	   return value;
   }


   /**
    * Parses the string to return its equivalent size in
    * bytes.
    * 
    * @param s
    * @return
    * @throws NumberFormatException
    */
   public static long parse(String s) throws NumberFormatException {
       return parse(s, ByteUnit.B);
   }

   /**
    * Parses the string to return its equivalent size in
    * bytes.
    *
    * @param s
    *          The size string to be parsed.
    * @param defaultUnit
    *          The default unit to use if there is no unit specified in
    *          the size string, or <code>null</code> if the string
    *          must always contain a unit.
    * @return Returns the parsed size in bytes.
    * @throws NumberFormatException
    *           If the provided size string could not be parsed.
    */
   public static long parse(String s, ByteUnit defaultUnit) throws NumberFormatException {

       // Value must be a floating point number followed by a unit.
       Pattern p = Pattern.compile("^\\s*(\\d+)\\s*(\\w+)?\\s*$");
       Matcher m = p.matcher(s);

       if (!m.matches()) {
           throw new NumberFormatException("Invalid size value \"" + s + "\"");
       }

       // Group 1 is the float.
       long l;

       try {
           l = Long.valueOf(m.group(1));
       } catch (NumberFormatException e) {
           throw new NumberFormatException("Invalid size value \"" + s + "\"");
       }

       // Group 2 is the unit.
       String unitString = m.group(2);
       ByteUnit unit;

       if (unitString == null) {
           if (defaultUnit == null) {
               throw new NumberFormatException("Invalid size value \"" + s + "\"");
           } else {
               unit = defaultUnit;
           }
       } else {
           try {
               unit = ByteUnit.valueOf(unitString.trim());
           } catch (IllegalArgumentException e) {
               throw new NumberFormatException("Invalid size value \"" + s + "\"");
           }
       }

       return l * unit.value;
   }
}