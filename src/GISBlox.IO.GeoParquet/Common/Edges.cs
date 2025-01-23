using System.Runtime.Serialization;

namespace GISBlox.IO.GeoParquet.Common
{
   /// <summary>
   /// This enum indicates how to interpret the edges of the geometries: whether the line between two points is a straight cartesian line or the shortest line on the sphere (geodesic line). 
   /// </summary>
   public enum Edges
   {
      /// <summary>
      /// Use a flat cartesian coordinate system.
      /// </summary>
      [EnumMember(Value = @"planar")]
      Planar = 0,

      /// <summary>
      /// Use a spherical coordinate system and radius derived from the spheroid defined by the coordinate reference system.
      /// </summary>
      [EnumMember(Value = @"spherical")]
      Spherical = 1,
   }
}
