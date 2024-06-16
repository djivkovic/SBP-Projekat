//1. Koji vremenski uslovi su najsmrtonosniji (collisions)

[
  {
    $lookup: {
      from: "victims",
      localField: "case_id",
      foreignField: "case_id",
      as: "victims"
    }
  },
  {
    $unwind: {
      path: "$victims",
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $addFields: {
      sorted_weather: {
        $cond: {
          if: {
            $and: [
              {
                $or: [
                  {
                    $eq: ["$weather_1", ""]
                  },
                  {
                    $eq: ["$weather_1", null]
                  }
                ]
              },
              {
                $or: [
                  {
                    $eq: ["$weather_2", ""]
                  },
                  {
                    $eq: ["$weather_2", null]
                  }
                ]
              }
            ]
          },
          then: "Unknown",
          else: {
            $cond: {
              if: {
                $and: [
                  {
                    $ne: ["$weather_1", ""]
                  },
                  {
                    $ne: ["$weather_2", ""]
                  }
                ]
              },
              then: {
                $concat: [
                  "$weather_1",
                  " and ",
                  "$weather_2"
                ]
              },
              else: {
                $ifNull: [
                  "$weather_1",
                  "$weather_2"
                ]
              }
            }
          }
        }
      }
    }
  },
  {
    $group: {
      _id: {
        case_id: "$case_id",
        sorted_weather: "$sorted_weather"
      },
      is_fatal: {
        $max: {
          $cond: [
            {
              $eq: [
                "$victims.victim_degree_of_injury",
                "killed"
              ]
            },
            1,
            0
          ]
        }
      }
    }
  },
  {
    $group: {
      _id: "$_id.sorted_weather",
      total_crashes: {
        $sum: 1
      },
      fatal_crashes: {
        $sum: "$is_fatal"
      },
      unique_case_ids: {
        $addToSet: "$_id.case_id"
      }
    }
  },
  {
    $project: {
      _id: 1,
      total_crashes: {
        $size: "$unique_case_ids"
      },
      fatal_crashes: 1,
      fatal_percentage: {
        $cond: {
          if: {
            $gt: ["$total_crashes", 0]
          },
          then: {
            $multiply: [
              {
                $divide: [
                  "$fatal_crashes",
                  "$total_crashes"
                ]
              },
              100
            ]
          },
          else: 0
        }
      }
    }
  },
  {
    $sort: {
      fatal_percentage: -1
    }
  }
]


//2. Koji je najcesci razlog sudara kod skolskih autobusa (collisions)

[
  {
    $lookup: {
      from: "parties",
      localField: "case_id",
      foreignField: "case_id",
      as: "parties"
    }
  },
  {
    $match: {
      "parties.statewide_vehicle_type": "schoolbus"
    }
  },
  {
    $group: {
      "_id": "$pcf_violation_category",
      "count": { "$sum": 1 }
    }
  },
  {
    $sort: { "count": -1 }
  },
  {
    $limit: 1
  }
]


//3. Koliko se svake godine povecao procenat udesa kod top 4 marke automobila ? (collisions)
[
  {
    "$lookup": {
      "from": "parties",
      "localField": "case_id",
      "foreignField": "case_id",
      "as": "parties"
    }
  },
  {
    "$unwind": "$parties"
  },
  {
    "$addFields": {
      "collision_year": { "$year": { "$toDate": "$collision_datetime" } }
    }
  },
  {
    "$group": {
      "_id": {
        "year": "$collision_year",
        "vehicle_make": "$parties.vehicle_make"
      },
      "total_collisions": { "$sum": 1 }
    }
  },
  {
    "$match": {
      "_id.year": { "$ne": null },
      "_id.vehicle_make": { "$ne": null }
    }
  },
  {
    "$sort": { "_id.year": 1 }
  },
  {
    "$group": {
      "_id": {
        "vehicle_make": "$_id.vehicle_make"
      },
      "collisions_by_year": {
        "$push": {
          "year": "$_id.year",
          "total_collisions": "$total_collisions"
        }
      }
    }
  },
  {
    "$addFields": {
      "percent_increase": {
        "$map": {
          "input": { "$range": [1, { "$size": "$collisions_by_year" }] },
          "in": {
            "year": { "$arrayElemAt": ["$collisions_by_year.year", "$$this"] },
            "percent_increase": {
              "$cond": {
                "if": {
                  "$gt": [
                    { "$arrayElemAt": ["$collisions_by_year.total_collisions", "$$this"] },
                    0
                  ]
                },
                "then": {
                  "$multiply": [
                    {
                      "$divide": [
                        {
                          "$subtract": [
                            { "$arrayElemAt": ["$collisions_by_year.total_collisions", "$$this"] },
                            { "$arrayElemAt": ["$collisions_by_year.total_collisions", { "$subtract": ["$$this", 1] }] }
                          ]
                        },
                        { "$arrayElemAt": ["$collisions_by_year.total_collisions", { "$subtract": ["$$this", 1] }] }
                      ]
                    },
                    100
                  ]
                },
                "else": 0
              }
            }
          }
        }
      }
    }
  },
  {
    "$sort": { "collisions_by_year.total_collisions": -1 }
  },
  {
    "$group": {
      "_id": "$_id.vehicle_make",
      "collisions_by_year": { "$first": "$collisions_by_year" },
      "percent_increase": { "$first": "$percent_increase" }
    }
  },
  {
    "$match": {
      "_id": { "$ne": "" }
    }
  },
  {
    "$sort": { "collisions_by_year.total_collisions": -1 }
  },
  {
    "$limit": 4
  },
  {
    "$project": {
      "_id": 0,
      "vehicle_make": "$_id",
      "collisions_by_year": 1,
      "percent_increase": 1
    }
  }
]


//4. Koji pol je kriv za najveci broj udesa leti i za koliko posto? (collisions)

[
  {
    $lookup: {
      from: "parties",
      localField: "case_id",
      foreignField: "case_id",
      as: "parties"
    }
  },
  {
    $unwind: "$parties"
  },
  {
    $addFields: {
      collision_month: {
        $month: {
          $toDate: "$collision_datetime"
        }
      }
    }
  },
  {
    $match: {
      collision_month: {
        $in: [6, 7, 8]
      },
      "parties.party_type": "driver",
      "parties.party_sex": {
        $ne: ""
      }
    }
  },
  {
    $group: {
      _id: "$parties.party_sex",
      total_collisions: {
        $sum: 1
      }
    }
  },
  {
    $group: {
      _id: null,
      total_all_collisions: {
        $sum: "$total_collisions"
      },
      results: {
        $push: {
          party_sex: "$_id",
          total_collisions: "$total_collisions"
        }
      }
    }
  },
  {
    $unwind: "$results"
  },
  {
    $addFields: {
      percentage_of_total: {
        $multiply: [
          {
            $divide: [
              "$results.total_collisions",
              "$total_all_collisions"
            ]
          },
          100
        ]
      }
    }
  },
  {
    $group: {
      _id: "$results.party_sex",
      total_collisions: {
        $first: "$results.total_collisions"
      },
      percentage_of_total: {
        $first: "$percentage_of_total"
      }
    }
  },
  {
    $sort: {
      total_collisions: -1
    }
  },
  {
    $project: {
      _id: 0,
      party_sex: "$_id",
      total_collisions: 1,
      percentage_of_total: {
        $round: ["$percentage_of_total", 2]
      }
    }
  }
]
  

//5. Procenat alkoholicara koji su imali osiguranje (collisions)
[
  {
    $lookup: {
      from: "parties",
      localField: "case_id",
      foreignField: "case_id",
      as: "party_details"
    }
  },
  {
    $unwind: "$party_details"
  },
  {
    $match: {
      "party_details.party_type": "driver",
      "party_details.party_sobriety":
        "had been drinking, under influence"
    }
  },
  {
    $group: {
      _id: "$party_details.party_sobriety",
      count: {
        $sum: 1
      },
      with_proof_of_insurance: {
        $sum: {
          $cond: [
            {
              $eq: [
                "$party_details.financial_responsibility",
                "proof of insurance obtained"
              ]
            },
            1,
            0
          ]
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      alcohol_involved: "$_id",
      count: 1,
      with_proof_of_insurance: 1,
      percentage_with_proof: {
        $multiply: [
          {
            $divide: [
              "$with_proof_of_insurance",
              "$count"
            ]
          },
          100
        ]
      }
    }
  }
]

//6. Koji dan je najsmrtonosniji (collisions)
[
  {
    $lookup: {
      from: "victims",
      localField: "case_id",
      foreignField: "case_id",
      as: "victim_details"
    }
  },
  {
    $unwind: "$victim_details"
  },
  {
    $addFields: {
      collision_date: {
        $dateFromString: {
          dateString: "$collision_datetime"
        }
      }
    }
  },
  {
    $addFields: {
      day_of_week_number: {
        $dayOfWeek: "$collision_date"
      }
    }
  },
  {
    $addFields: {
      day_of_week: {
        $switch: {
          branches: [
            {
              case: {
                $eq: ["$day_of_week_number", 1]
              },
              then: "Sunday"
            },
            {
              case: {
                $eq: ["$day_of_week_number", 2]
              },
              then: "Monday"
            },
            {
              case: {
                $eq: ["$day_of_week_number", 3]
              },
              then: "Tuesday"
            },
            {
              case: {
                $eq: ["$day_of_week_number", 4]
              },
              then: "Wednesday"
            },
            {
              case: {
                $eq: ["$day_of_week_number", 5]
              },
              then: "Thursday"
            },
            {
              case: {
                $eq: ["$day_of_week_number", 6]
              },
              then: "Friday"
            },
            {
              case: {
                $eq: ["$day_of_week_number", 7]
              },
              then: "Saturday"
            }
          ],
          default: "Unknown"
        }
      }
    }
  },
  {
    $group: {
      _id: {
        day_of_week: "$day_of_week",
        case_id: "$case_id"
      },
      total_victims: {
        $sum: 1
      },
      killed_victims: {
        $sum: {
          $cond: [
            {
              $eq: [
                "$victim_details.victim_degree_of_injury",
                "killed"
              ]
            },
            1,
            0
          ]
        }
      }
    }
  },
  {
    $group: {
      _id: "$_id.day_of_week",
      total_crashes: {
        $sum: 1
      },
      total_victims: {
        $sum: "$total_victims"
      },
      killed_victims: {
        $sum: "$killed_victims"
      }
    }
  },
  {
    $addFields: {
      percentage_killed_victims: {
        $multiply: [
          {
            $divide: [
              "$killed_victims",
              "$total_victims"
            ]
          },
          100
        ]
      }
    }
  },
  {
    $sort: {
      percentage_killed_victims: -1
    }
  },
  {
    $match: {
      _id: {
        $ne: "Unknown"
      }
    }
  },
  {
    $project: {
      day_of_week: "$_id",
      total_crashes: 1,
      total_victims: 1,
      killed_victims: 1,
      percentage_killed_victims: 1
    }
  }
]

//7. Most Fatal Collisions by Type (collisions)

[
  {
    $lookup: {
      from: "victims",
      localField: "case_id",
      foreignField: "case_id",
      as: "victim_details"
    }
  },
  {
    $unwind: "$victim_details"
  },
  {
    $group: {
      _id: {
        type_of_collision: "$type_of_collision",
        case_id: "$case_id"
      },
      total_victims: {
        $sum: 1
      },
      killed_victims: {
        $sum: {
          $cond: [
            {
              $eq: [
                "$victim_details.victim_degree_of_injury",
                "killed"
              ]
            },
            1,
            0
          ]
        }
      }
    }
  },
  {
    $group: {
      _id: "$_id.type_of_collision",
      total_collisions: {
        $sum: 1
      },
      total_victims: {
        $sum: "$total_victims"
      },
      killed_victims: {
        $sum: "$killed_victims"
      }
    }
  },
  {
    $addFields: {
      lethality_percentage: {
        $multiply: [
          {
            $divide: [
              "$killed_victims",
              "$total_victims"
            ]
          },
          100
        ]
      }
    }
  },
  {
    $sort: {
      lethality_percentage: -1
    }
  },
  {
    $match: {
      _id: {
        $ne: ""
      }
    }
  },
  {
    $project: {
      type_of_collision: "$_id",
      total_collisions: 1,
      total_victims: 1,
      killed_victims: 1,
      lethality_percentage: 1
    }
  }
]


//8. Number of Solo Motorcycle Crashes per Year (collisions)
[
  {
    $lookup: {
      from: "parties",
      localField: "case_id",
      foreignField: "case_id",
      as: "party_details"
    }
  },
  {
    $lookup: {
      from: "victims",
      localField: "case_id",
      foreignField: "case_id",
      as: "victim_details"
    }
  },
  {
    $addFields: {
      year: {
        $year: {
          $dateFromString: {
            dateString: "$collision_datetime"
          }
        }
      }
    }
  },
  {
    $match: {
      "party_details.statewide_vehicle_type":
        "motorcycle or scooter",
      year: {
        $ne: null
      }
    }
  },
  {
    $group: {
      _id: "$case_id",
      year: {
        $first: "$year"
      },
      num_parties: {
        $sum: 1
      },
      num_victims: {
        $sum: {
          $size: "$victim_details"
        }
      }
    }
  },
  {
    $match: {
      num_parties: {
        $lte: 1
      },
      num_victims: {
        $lte: 1
      }
    }
  },
  {
    $group: {
      _id: "$year",
      solo_motorcycle_crashes: {
        $sum: 1
      }
    }
  },
  {
    $project: {
      year: "$_id",
      solo_motorcycle_crashes: 1
    }
  },
  {
    $sort: {
      year: 1
    }
  }
]
//9. Za koje vreme treba upozoriti vozace kada je najopasnije voziti i pod kojim osvetljenjem (collisions)
[
  {
    $lookup: {
      from: "victims",
      localField: "case_id",
      foreignField: "case_id",
      as: "victim_details"
    }
  },
  {
    $unwind: "$victim_details"
  },
  {
    $match: {
      lighting: {
        $ne: ""
      },
      collision_datetime: {
        $ne: null
      }
    }
  },
  {
    $addFields: {
      collision_date: {
        $dateFromString: {
          dateString: "$collision_datetime"
        }
      }
    }
  },
  {
    $addFields: {
      time_of_day: {
        $dateToString: {
          format: "%H:%M",
          date: "$collision_date"
        }
      }
    }
  },
  {
    $addFields: {
      time_segment: {
        $switch: {
          branches: [
            {
              case: {
                $lte: [
                  {
                    $minute: "$collision_date"
                  },
                  30
                ]
              },
              then: {
                $concat: [
                  {
                    $substr: [
                      {
                        $hour: "$collision_date"
                      },
                      0,
                      2
                    ]
                  },
                  ":00"
                ]
              }
            },
            {
              case: {
                $gt: [
                  {
                    $minute: "$collision_date"
                  },
                  30
                ]
              },
              then: {
                $concat: [
                  {
                    $substr: [
                      {
                        $hour: "$collision_date"
                      },
                      0,
                      2
                    ]
                  },
                  ":30"
                ]
              }
            }
          ],
          default: "Unknown"
        }
      }
    }
  },
  {
    $addFields: {
      month: {
        $month: "$collision_date"
      }
    }
  },
  {
    $addFields: {
      season: {
        $switch: {
          branches: [
            {
              case: {
                $in: ["$month", [12, 1, 2]]
              },
              then: "Winter"
            },
            {
              case: {
                $in: ["$month", [3, 4, 5]]
              },
              then: "Spring"
            },
            {
              case: {
                $in: ["$month", [6, 7, 8]]
              },
              then: "Summer"
            },
            {
              case: {
                $in: ["$month", [9, 10, 11]]
              },
              then: "Fall"
            }
          ],
          default: "Unknown"
        }
      }
    }
  },
  {
    $group: {
      _id: {
        lighting: "$lighting",
        time_segment: "$time_segment",
        season: "$season",
        case_id: "$case_id"
      },
      total_victims: {
        $sum: 1
      },
      killed_victims: {
        $sum: {
          $cond: [
            {
              $eq: [
                "$victim_details.victim_degree_of_injury",
                "killed"
              ]
            },
            1,
            0
          ]
        }
      }
    }
  },
  {
    $group: {
      _id: {
        lighting: "$_id.lighting",
        time_segment: "$_id.time_segment",
        season: "$_id.season"
      },
      total_collisions: {
        $sum: 1
      },
      total_victims: {
        $sum: "$total_victims"
      },
      killed_victims: {
        $sum: "$killed_victims"
      }
    }
  },
  {
    $addFields: {
      lethality_percentage: {
        $multiply: [
          {
            $divide: [
              "$killed_victims",
              "$total_victims"
            ]
          },
          100
        ]
      }
    }
  },
  {
    $sort: {
      total_collisions: -1
    }
  },
  {
    $project: {
      lighting: "$_id.lighting",
      time_segment: "$_id.time_segment",
      season: "$_id.season",
      total_collisions: 1,
      total_victims: 1,
      killed_victims: 1,
      lethality_percentage: 1
    }
  }
]

//10. Najveci udes koji postoji sa detaljima o svakom ucesniku i povredjenoj osobi (collisions)
[
  {
    $lookup: {
      from: "parties",
      localField: "case_id",
      foreignField: "case_id",
      as: "party_details"
    }
  },
  {
    $project: {
      case_id: 1,
      collision_datetime: 1,
      officer_id: 1,
      county_location: 1,
      primary_road: 1,
      secondary_road: 1,
      distance: 1,
      intersection: {
        $cond: {
          if: {
            $eq: ["$intersection", true]
          },
          then: {
            $concat: [
              "$primary_road",
              " & ",
              "$secondary_road"
            ]
          },
          else: null
        }
      },
      weather_1: 1,
      weather_2: 1,
      tow_away: 1,
      primary_collision_factor: 1,
      pcf_violation_category: 1,
      hit_and_run: 1,
      type_of_collision: 1,
      motor_vehicle_involved_with: 1,
      pedestrian_action: 1,
      road_surface: 1,
      road_condition_1: 1,
      road_condition_2: 1,
      lighting: 1,
      alcohol_involved: 1,
      party_details: {
        $filter: {
          input: "$party_details",
          as: "party",
          cond: {
            $eq: ["$$party.case_id", "$case_id"]
          }
        }
      }
    }
  },
  {
    $addFields: {
      num_participants: {
        $size: "$party_details"
      }
    }
  },
  {
    $sort: {
      num_participants: -1
    }
  },
  {
    $limit: 1
  },
  {
    $lookup: {
      from: "victims",
      localField: "case_id",
      foreignField: "case_id",
      as: "victim_details"
    }
  },
  {
    $project: {
      case_id: 1,
      collision_datetime: 1,
      officer_id: 1,
      county_location: 1,
      primary_road: 1,
      secondary_road: 1,
      distance: 1,
      intersection: 1,
      weather_1: 1,
      weather_2: 1,
      tow_away: 1,
      primary_collision_factor: 1,
      pcf_violation_category: 1,
      hit_and_run: 1,
      type_of_collision: 1,
      motor_vehicle_involved_with: 1,
      pedestrian_action: 1,
      road_surface: 1,
      road_condition_1: 1,
      road_condition_2: 1,
      lighting: 1,
      alcohol_involved: 1,
      party_details: 1,
      victim_details: {
        $filter: {
          input: "$victim_details",
          as: "victim",
          cond: {
            $eq: ["$$victim.case_id", "$case_id"]
          }
        }
      }
    }
  },
  {
    $addFields: {
      num_victims: {
        $size: "$victim_details"
      }
    }
  }
]