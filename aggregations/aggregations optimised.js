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
    $group: {
      _id: {
        case_id: "$case_id",
        weather: "$conditions.weather"
      },
      total_victims: {
        $sum: "$victims.total_victims"
      },
      total_lethal: {
        $sum: "$victims.total_lethal"
      }
    }
  },
  {
    $group: {
      _id: "$_id.weather",
      total_crashes: {
        $sum: 1
      },
      total_victims: {
        $sum: "$total_victims"
      },
      total_lethal: {
        $sum: "$total_lethal"
      }
    }
  },
  {
    $project: {
      _id: 1,
      total_crashes: 1,
      total_victims: 1,
      total_lethal: 1,
      fatal_percentage: {
        $cond: {
          if: {
            $gt: ["$total_crashes", 0]
          },
          then: {
            $multiply: [
              {
                $divide: [
                  "$total_lethal",
                  "$total_victims"
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
      "parties.parties.statewide_vehicle_type":
        "schoolbus"
    }
  },
  {
    $group: {
      _id: "$pcf_violation_category",
      count: {
        $sum: 1
      }
    }
  },
  {
    $sort: {
      count: -1
    }
  },
  {
    $limit: 1
  }
]

//3. Koliko se svake godine povecao procenat udesa kod top 4 marke automobila ? (collisions)
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
    $unwind: "$parties.parties"
  },
  {
    $addFields: {
      collision_year: {
        $year: {
          $toDate: "$datetime.datetime"
        }
      }
    }
  },
  {
    $group: {
      _id: {
        year: "$collision_year",
        vehicle_make:
          "$parties.parties.vehicle_make"
      },
      total_collisions: {
        $sum: 1
      }
    }
  },
  {
    $match: {
      "_id.year": {
        $ne: null
      },
      "_id.vehicle_make": {
        $ne: null
      }
    }
  },
  {
    $sort: {
      "_id.year": 1
    }
  },
  {
    $group: {
      _id: "$_id.vehicle_make",
      collisions_by_year: {
        $push: {
          year: "$_id.year",
          total_collisions: "$total_collisions"
        }
      }
    }
  },
  {
    $addFields: {
      percent_increase: {
        $map: {
          input: {
            $range: [
              1,
              {
                $size: "$collisions_by_year"
              }
            ]
          },
          as: "index",
          in: {
            year: {
              $arrayElemAt: [
                "$collisions_by_year.year",
                "$$index"
              ]
            },
            percent_increase: {
              $cond: {
                if: {
                  $gt: [
                    {
                      $arrayElemAt: [
                        "$collisions_by_year.total_collisions",
                        "$$index"
                      ]
                    },
                    0
                  ]
                },
                then: {
                  $multiply: [
                    {
                      $divide: [
                        {
                          $subtract: [
                            {
                              $arrayElemAt: [
                                "$collisions_by_year.total_collisions",
                                "$$index"
                              ]
                            },
                            {
                              $arrayElemAt: [
                                "$collisions_by_year.total_collisions",
                                {
                                  $subtract: [
                                    "$$index",
                                    1
                                  ]
                                }
                              ]
                            }
                          ]
                        },
                        {
                          $arrayElemAt: [
                            "$collisions_by_year.total_collisions",
                            {
                              $subtract: [
                                "$$index",
                                1
                              ]
                            }
                          ]
                        }
                      ]
                    },
                    100
                  ]
                },
                else: 0
              }
            }
          }
        }
      }
    }
  },
  {
    $sort: {
      "collisions_by_year.total_collisions": -1
    }
  },
  {
    $group: {
      _id: "$_id",
      collisions_by_year: {
        $first: "$collisions_by_year"
      },
      percent_increase: {
        $first: "$percent_increase"
      }
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
    $sort: {
      "collisions_by_year.total_collisions": -1
    }
  },
  {
    $limit: 4
  },
  {
    $project: {
      _id: 0,
      vehicle_make: "$_id",
      collisions_by_year: 1,
      percent_increase: 1
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
    $unwind: "$parties.parties"
  },
  {
    $match: {
      "datetime.season": "Summer",
      "parties.parties.party_type": "driver",
      "parties.parties.party_sex": {
        $ne: ""
      }
    }
  },
  {
    $group: {
      _id: "$parties.parties.party_sex",
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
    $unwind: "$party_details.parties"
  },
  {
    $match: {
      "party_details.parties.party_type":
        "driver",
      "party_details.parties.party_sobriety":
        "had been drinking, under influence"
    }
  },
  {
    $group: {
      _id: "$party_details.parties.party_sobriety",
      count: {
        $sum: 1
      },
      with_proof_of_insurance: {
        $sum: {
          $cond: [
            {
              $eq: [
                "$party_details.parties.financial_responsibility",
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
      as: "victims"
    }
  },
  {
    $unwind: "$victims"
  },
  {
    $group: {
      _id: {
        day_of_week: "$datetime.day_of_week",
        case_id: "$case_id"
      },
      total_victims: {
        $sum: "$victims.total_victims"
      },
      killed_victims: {
        $sum: "$victims.total_lethal"
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
    $match: {
      _id: {
        $ne: null
      }
    }
  },
  {
    $addFields: {
      percentage_killed_victims: {
        $multiply: [
          {
            $cond: [
              {
                $gt: ["$total_victims", 0]
              },
              {
                $divide: [
                  "$killed_victims",
                  "$total_victims"
                ]
              },
              0
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
    $project: {
      _id: 0,
      day_of_week: "$_id",
      total_crashes: 1,
      total_victims: 1,
      killed_victims: 1,
      percentage_killed_victims: {
        $round: ["$percentage_killed_victims", 2]
      }
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
      as: "victims"
    }
  },
  {
    $unwind: "$victims"
  },
  {
    $group: {
      _id: {
        type_of_collision: "$type_of_collision",
        case_id: "$case_id"
      },
      total_victims: {
        $sum: "$victims.total_victims"
      },
      killed_victims: {
        $sum: "$victims.total_lethal"
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
        $cond: {
          if: {
            $gt: ["$total_victims", 0]
          },
          then: {
            $multiply: [
              {
                $divide: [
                  "$killed_victims",
                  "$total_victims"
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
      _id: 0,
      type_of_collision: "$_id",
      total_collisions: 1,
      total_victims: 1,
      killed_victims: 1,
      lethality_percentage: {
        $round: ["$lethality_percentage", 2]
      }
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
      as: "parties"
    }
  },
  {
    $lookup: {
      from: "victims",
      localField: "case_id",
      foreignField: "case_id",
      as: "victims"
    }
  },
  {
    $addFields: {
      year: {
        $year: {
          $toDate: "$datetime.datetime"
        }
      }
    }
  },
  {
    $match: {
      "parties.parties.statewide_vehicle_type":
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
        $first: {
          $size: "$parties.total_parties"
        }
      },
      num_victims: {
        $first: {
          $size: "$victims.total_victims"
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
      as: "victims"
    }
  },
  {
    $unwind: "$victims"
  },
  {
    $addFields: {
      collision_date: {
        $dateFromString: {
          dateString: "$datetime.datetime"
        }
      }
    }
  },
  {
    $match: {
      lighting: {
        $ne: ""
      },
      collision_date: {
        $ne: null
      }
    }
  },
  {
    $addFields: {
      time_of_day: {
        $concat: [
          {
            $substr: [
              {
                $hour: "$collision_date"
              },
              0,
              -1
            ]
          },
          ":",
          {
            $cond: [
              {
                $lte: [
                  {
                    $minute: "$collision_date"
                  },
                  30
                ]
              },
              "00",
              "30"
            ]
          }
        ]
      }
    }
  },
  {
    $group: {
      _id: {
        lighting: "$lighting",
        time_segment: "$time_of_day",
        season: "$datetime.season",
        case_id: "$case_id"
      },
      total_victims: {
        $sum: "$victims.total_victims"
      },
      killed_victims: {
        $sum: "$victims.total_lethal"
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
        $cond: {
          if: {
            $gt: ["$total_victims", 0]
          },
          then: {
            $multiply: [
              {
                $divide: [
                  "$killed_victims",
                  "$total_victims"
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
      lethality_percentage: {
        $round: ["$lethality_percentage", 2]
      }
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
      datetime: 1,
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
      conditions: 1,
      tow_away: 1,
      primary_collision_factor: 1,
      pcf_violation_category: 1,
      hit_and_run: 1,
      type_of_collision: 1,
      motor_vehicle_involved_with: 1,
      pedestrian_action: 1,
      alcohol_involved: 1,
      party_details: {
        $filter: {
          input: "$party_details",
          as: "party",
          cond: {
            $eq: ["$$party.case_id", "$case_id"]
          }
        }
      },
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
      num_participants:
        "$party_details.total_parties",
      num_victims: "$victim_details.total_victims"
    }
  },
  {
    $addFields: {
      num_participants: {
        $cond: {
          if: {
            $gt: [
              {
                $size: "$num_participants"
              },
              0
            ]
          },
          then: {
            $arrayElemAt: ["$num_participants", 0]
          },
          else: 0
        }
      },
      num_victims: {
        $cond: {
          if: {
            $gt: [
              {
                $size: "$num_victims"
              },
              0
            ]
          },
          then: {
            $arrayElemAt: ["$num_victims", 0]
          },
          else: 0
        }
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
  }
]