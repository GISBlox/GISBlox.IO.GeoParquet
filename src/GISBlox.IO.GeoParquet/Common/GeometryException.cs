namespace GISBlox.IO.GeoParquet.Common
{
   /// <summary>
   /// Exception thrown when an error occurs in the geometry processing.
   /// </summary>
   public class GeometryException : Exception
   {
      /// <summary>
      /// Creates a new instance of the <see cref="GeometryException"/> class.
      /// </summary>
      public GeometryException()
      {
      }

      /// <summary>
      /// Creates a new instance of the <see cref="GeometryException"/> class with a specified error message.
      /// </summary>
      /// <param name="message">The error message.</param>
      public GeometryException(string message) : base(message)
      {
      }

      /// <summary>
      /// Creates a new instance of the <see cref="GeometryException"/> class with a specified error message and a reference to the inner exception that is the cause of this exception.
      /// </summary>
      /// <param name="message">The error message.</param>
      /// <param name="innerException">The inner exception.</param>
      public GeometryException(string message, Exception innerException) : base(message, innerException)
      {
      }      
   }
}